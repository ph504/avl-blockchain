package approach4.temporal.skipList;

import approach4.IAuthDigest;
import approach4.IRowDetails;
import approach4.Utils;
import approach4.temporal.temporalPartitions.PartitionSearchRes;
import approach4.valueDataStructures.Version;

import java.util.ArrayList;
import java.util.Random;
import java.util.Stack;


public class ToweredSkipList<KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K,V,KVER>> {

	public static int MAX_LEVEL = 10;

	private final int partitionCapacity;

	private KVER currentVersion;

	private final double iterationProbability;

	// An instance of the random number generator.
	// TODO remove seed
	public static Random random = new Random();

	// no tail, as tail will be always null
	private final Tower<KVER,K,V> head;

	private final ToweredTypeUtils<K,V> toweredTypeUtils;


	public ToweredSkipList(KVER initVersion, double iterationProbability, int partitionCapacity, ToweredTypeUtils<K, V> toweredTypeUtils) throws Exception {
		this.currentVersion = initVersion;
		this.iterationProbability = iterationProbability;
		this.partitionCapacity = partitionCapacity;
		this.head = new Tower<>(Tower.TowerType.HEAD, initVersion, null, 0, this.partitionCapacity);
		this.head.processLevelDigest(0, null, toweredTypeUtils);
		this.toweredTypeUtils = toweredTypeUtils;
	}

	public byte[] getRootDigest() throws Exception {
		return this.head.getDigest(this.head.maxLevel);
	}

	public Tower<KVER,K,V> getHead() {
		return this.head;
	}

	public void commitCurrentVersion(KVER nextVersion) throws Exception {
		Utils.checkVersions(this.currentVersion, nextVersion);
		this.currentVersion = nextVersion;
	}

	// for each level calculate the digest to draw forth the digest of tower
	public void processTowerDigests(int maxLevel, Tower<KVER,K,V> tower) throws Exception {
		for (int level = 0; level <= maxLevel; level++) {
			Tower<KVER,K,V> pointedTower = tower.getPointedTower(level);
			tower.processLevelDigest(level, pointedTower, this.toweredTypeUtils);
		}
	}

	private Tower<KVER,K,V> processLowerLevelsForUpsert(int level, Tower<KVER,K,V> tower, Tower<KVER,K,V> newTower) throws Exception {

		Tower<KVER,K,V> existingTower = null;
		if (level >= 0 && tower != null) {
			Tower<KVER, K, V> pointedTower = tower.getPointedTower(level);

			if (pointedTower == null) {
				existingTower = processLowerLevelsForUpsert(level - 1, tower, newTower);
				tower.processLevelDigest(level, null, this.toweredTypeUtils);
			} else {
				int pointedTowerKeyCompareToNewTowerKey = pointedTower.key.compareTo(newTower.key);
				if (pointedTowerKeyCompareToNewTowerKey < 0) {
					// need to traverse to the right most tower before the inserted tower with the bigger key
					existingTower = processLowerLevelsForUpsert(level, pointedTower, newTower);
					tower.processLevelDigest(level, pointedTower, this.toweredTypeUtils);
				} else if (pointedTowerKeyCompareToNewTowerKey > 0) {
					// current tower key is smaller than key. next tower key is bigger than key
					existingTower = processLowerLevelsForUpsert(level - 1, tower, newTower);
					if (existingTower == null) {
						newTower.setPointedTower(level, pointedTower);
						tower.setPointedTower(level, null);
						tower.processLevelDigest(level, null, this.toweredTypeUtils);
					} else {
						tower.processLevelDigest(level, pointedTower, this.toweredTypeUtils);
//						throw new Exception("should not get here");
					}
				} else {
					existingTower = pointedTower;
					V row = newTower.value.getLastRow();
					Utils.assertNotNull(row, "should not be null");
					row.initVersions();
					updateTower(row, existingTower);
					tower.processLevelDigest(level, pointedTower, this.toweredTypeUtils);
				}
			}
		}

		return existingTower;
	}

	// dirty code, imo, at least has some debugging gimicks but in the wrong place.
	public static <KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K,V,KVER>>
	Tower<KVER,K,V> getNewTower(KVER version, V row, Integer newTowerMaxLevel, int partitionCapacity) throws Exception {
		if (newTowerMaxLevel == null) {
			throw new Exception("newTowerMaxLevel should not be null");
		}
		Tower<KVER,K,V> tower = new Tower<>(version, row, newTowerMaxLevel, partitionCapacity);
		return tower;
	}

	public void upsert(V row) throws Exception {
		upsert(row, null);
	}

	public void upsert(V row, Integer newTowerMaxLevel) throws Exception {
//		Utils.assertNotNull(row,"should not be null");
		K key = row.getKey();
//		Utils.assertNotNull(key,"should not be null");

		if (newTowerMaxLevel == null) {
			newTowerMaxLevel = Utils.getRandomLevel(MAX_LEVEL, random, iterationProbability);
		}

//		System.out.println("inserting key: " + newTower.key + " value: " + newTower.value + " towerLevel: " + newTower.maxLevel);
		this.head.extendHeadTowerMaxLevel(newTowerMaxLevel, this.toweredTypeUtils);

		upsertInner(this.head.maxLevel, this.head, key, row, newTowerMaxLevel);
	}

	public void upsertInner(int level, Tower<KVER, K, V> tower, K key, V row, Integer newTowerMaxLevel) throws Exception {
		if (level >= 0 && tower != null) {
			Tower<KVER, K, V> pointedTower = tower.getPointedTower(level);

			if (pointedTower == null) {
				insertTower(level, tower, null, key, row, newTowerMaxLevel);
			} else {
				int pointedTowerKeyCompareToSearchKey = pointedTower.key.compareTo(key);
				if (pointedTowerKeyCompareToSearchKey == 0) {
					updateTower(row, pointedTower);
					tower.processLevelDigest(level, pointedTower, this.toweredTypeUtils);
				} else if (pointedTowerKeyCompareToSearchKey > 0) {
					insertTower(level, tower, pointedTower, key, row, newTowerMaxLevel);
				} else {
					// here: pointedTower.key.compareTo(key) < 0
					if (level >= newTowerMaxLevel) {
						upsertInner(level, pointedTower, key, row, newTowerMaxLevel);
						tower.processLevelDigest(level, pointedTower, this.toweredTypeUtils);
					} else {
						// here:
						// pointedTower.key.compareTo(newTower.key) < 0
						// level < newTowerMaxLevel
						throw new Exception("should not get here");
					}
				}
			}
		}
	}

	private void insertTower(int level, Tower<KVER, K, V> tower, Tower<KVER, K, V> pointedTower, K key, V row, Integer newTowerMaxLevel) throws Exception {
		if (level == newTowerMaxLevel) {
			Tower<KVER, K, V> newTower = getNewTower(this.currentVersion, row, newTowerMaxLevel, this.partitionCapacity);
			Tower<KVER, K, V> existingTower = processLowerLevelsForUpsert(level - 1, tower, newTower);
			if (existingTower == null) {
				newTower.setPointedTower(level, pointedTower);
				processTowerDigests(level, newTower);
				tower.setPointedTower(level, newTower);
				tower.processLevelDigest(level, newTower, this.toweredTypeUtils);
			} else {
				tower.processLevelDigest(level, pointedTower, this.toweredTypeUtils);
			}
		} else if (level > newTowerMaxLevel) {
			upsertInner(level - 1, tower, key, row, newTowerMaxLevel);
			tower.processLevelDigest(level, pointedTower, this.toweredTypeUtils);
		} else {
			throw new Exception("should not get here");
		}
	}

	public void updateTower(V row, Tower<KVER,K,V> towerToUpdate) throws Exception {
		Utils.assertTrue(!towerToUpdate.value.isEmpty(), "should be true");

		V lastRow = towerToUpdate.value.getLastRow();
		Version<KVER> lastRowVersion = lastRow.getVersion();
		if (lastRowVersion.getValidTo() == null) {
//			throw new Exception("a row with the same key already exists");
			V updatedRow = towerToUpdate.value.updateLastRow(this.currentVersion, row);
		} else {
			Utils.assertTrue(lastRowVersion.getValidFrom().compareTo(this.currentVersion)  < 0);
			V addedRow = towerToUpdate.value.add(this.currentVersion, row);
		}
		processTowerDigests(towerToUpdate.maxLevel, towerToUpdate);
	}

	public V updateTower_(V row, Tower<KVER, K, V> towerToUpdate) throws Exception {
		Utils.assertTrue(!towerToUpdate.value.isEmpty(), "should be true");
		V updatedRow = towerToUpdate.value.updateLastRow(this.currentVersion, row);
		processTowerDigests(towerToUpdate.maxLevel, towerToUpdate);

		return updatedRow;
	}

	public V update(V row) throws Exception {
		Utils.assertNotNull(row,"should not be null");

		K key = row.getKey();

//		System.out.println("inserting key: " + newTower.key + " value: " + newTower.value + " towerLevel: " + newTower.maxLevel);
		this.head.extendHeadTowerMaxLevel(0, this.toweredTypeUtils);

		V updatedRow = updateInner(this.head.maxLevel, this.head, key, row);
		return updatedRow;
	}

	public V updateInner(int level, Tower<KVER, K, V> tower, K key, V row) throws Exception {
		V updatedRow = null;
		if (level >= 0 && tower != null) {
			Tower<KVER,K,V> pointedTower = tower.getPointedTower(level);

			if (pointedTower == null) {
				updatedRow = updateInner(level - 1, tower, key, row);
			} else {
				int pointedTowerKeyCompareToSearchKey = pointedTower.key.compareTo(key);
				if (pointedTowerKeyCompareToSearchKey == 0) {
					updatedRow = updateTower_(row, pointedTower);
				} else if (pointedTowerKeyCompareToSearchKey > 0) {
					if (level > 0) {
						updatedRow = updateInner(level - 1, tower, key, row);
					} else {
						throw new Exception("should not get here");
					}
				} else {
					// here: pointedTower.key.compareTo(newTower.key) < 0
					updatedRow = updateInner(level, pointedTower, key, row);
				}
			}

			if (updatedRow != null) {
				tower.processLevelDigest(level, pointedTower, this.toweredTypeUtils);
			}
		}
		return updatedRow;
	}

	private void processLowerTowerLevelsForDelete(int level, Tower<KVER,K,V> tower, Tower<KVER,K,V> towerToDelete) throws Exception {
		if (level >= 0 && tower != null) {
			Tower<KVER, K, V> pointedTower = tower.getPointedTower(level);

			if (pointedTower == null) {
				Tower<KVER, K, V> towerToDeletePointedTower = towerToDelete.getPointedTower(level);
				processLowerTowerLevelsForDelete(level - 1, tower, towerToDelete);
				tower.setPointedTower(level, towerToDeletePointedTower);
				tower.processLevelDigest(level, towerToDeletePointedTower, this.toweredTypeUtils);
			} else {
				processLowerTowerLevelsForDelete(level, pointedTower, towerToDelete);
				tower.setPointedTower(level, pointedTower);
				tower.processLevelDigest(level, pointedTower, this.toweredTypeUtils);
			}
		}
	}

	public V deleteTower(int level, Tower<KVER, K, V> tower, Tower<KVER, K, V> towerToDelete) throws Exception {
		V deletedRow = towerToDelete.value.deleteLastRow(this.currentVersion);

		if (towerToDelete.value.isEmpty()) {
			processLowerTowerLevelsForDelete(towerToDelete.maxLevel - 1, tower, towerToDelete);

			Tower<KVER, K, V> pointedPointedTower = towerToDelete.getPointedTower(level);
			tower.setPointedTower(level, pointedPointedTower);
			tower.processLevelDigest(level, pointedPointedTower, this.toweredTypeUtils);
		} else {
			processTowerDigests(towerToDelete.maxLevel, towerToDelete);
			tower.processLevelDigest(level, towerToDelete, this.toweredTypeUtils);
		}
		return deletedRow;
	}

	public V delete(K key) throws Exception {
		Utils.assertNotNull(key,"should not be null");

//		System.out.println("inserting key: " + newTower.key + " value: " + newTower.value + " towerLevel: " + newTower.maxLevel);
		this.head.extendHeadTowerMaxLevel(0, this.toweredTypeUtils);

		V deletedRow = deleteInner(this.head.maxLevel, this.head, key);
		return deletedRow;
	}

	public V deleteInner(int level, Tower<KVER, K, V> tower, K key) throws Exception {
		V deletedRow = null;
		if (level >= 0 && tower != null) {
			Tower<KVER,K,V> pointedTower = tower.getPointedTower(level);

			if (pointedTower == null) {
				deletedRow = deleteInner(level - 1, tower, key);
				if (deletedRow != null) {
					tower.processLevelDigest(level, pointedTower, this.toweredTypeUtils);
				}
			} else {
				int pointedTowerKeyCompareToSearchKey = pointedTower.key.compareTo(key);
				if (pointedTowerKeyCompareToSearchKey == 0) {
					deletedRow = deleteTower(level, tower, pointedTower);
					if (deletedRow != null) {
						Tower<KVER, K, V> deletedTowerPointedTower = pointedTower.getPointedTower(level);
						tower.processLevelDigest(level, deletedTowerPointedTower, this.toweredTypeUtils);
					}
				} else if (pointedTowerKeyCompareToSearchKey > 0) {
					if (level > 0) {
						deletedRow = deleteInner(level - 1, tower, key);
						if (deletedRow != null) {
							tower.processLevelDigest(level, pointedTower, this.toweredTypeUtils);
						}
					} else {
						throw new Exception("should not get here");
					}
				} else {
					// here: pointedTower.key.compareTo(newTower.key) < 0
					deletedRow = deleteInner(level, pointedTower, key);
					if (deletedRow != null) {
						tower.processLevelDigest(level, pointedTower, this.toweredTypeUtils);
					}
				}
			}
		}

		return deletedRow;
	}


	public V search1(KVER version, K key) throws Exception {
//		Utils.assertNotNull(key, "key null");
//		Utils.assertTrue(this.head.maxLevel >= 0, "this.head.maxLevel < 0");

		Tower<KVER,K,V> tower = this.head;
		int level = tower.maxLevel;

		while (level >= 0) {
			Tower<KVER,K,V> pointedTower = tower.getPointedTower(level);

			if (pointedTower == null) {
				level--;
			} else {
				int cmp = pointedTower.key.compareTo(key);
				if (cmp > 0) {
					level--;
				} else if (cmp == 0) {
					return pointedTower.value.search(version);
				} else {
					tower = pointedTower;
				}
			}
		}

		return null;
	}

	public AuthSearchResults<KVER,K,V> authenticatedSearch(KVER version, K key) throws Exception {
		Utils.assertNotNull(key, "key null");
		Utils.assertTrue(this.head.maxLevel >= 0, "this.head.maxLevel < 0");

		Tower<KVER,K,V> tower = this.head;
		int level = tower.maxLevel;

		ArrayList<IAuthDigest> authDigests = new ArrayList<>();
		AuthSearchResults<KVER, K, V> res = null;
		while (level >= 0) {
			Tower<KVER,K,V> pointedTower = tower.getPointedTower(level);

			if (pointedTower == null) {
				if (level > 0) {
					byte[] digest = Utils.nullDigest;
					AuthUpperLevelDigest toweredUpperLevelDigest = new AuthUpperLevelDigest(digest);
					authDigests.add(toweredUpperLevelDigest);

					level--;
				} else {
					authDigests = getAuthDigestsOfFoundKey(version, key, tower, authDigests);
					if (authDigests != null) {
						res = new AuthSearchResults<>(authDigests);
					}
					break;
				}
			} else {
				if (pointedTower.key.compareTo(key) > 0) {
					if (level > 0) {
						byte[] digest = Tower.getNullableTowerDigest(level, pointedTower);
						AuthUpperLevelDigest toweredUpperLevelDigest = new AuthUpperLevelDigest(digest);
						authDigests.add(toweredUpperLevelDigest);

						level--;
					} else {
						authDigests = getAuthDigestsOfFoundKey(version, key, tower, authDigests);
						if (authDigests != null) {
							res = new AuthSearchResults<>(authDigests);
						}
						break;
					}
				} else {
					if (level > 0) {
						byte[] digest = tower.getBottomLevelDigest(level);
						AuthUpperLevelDigest toweredUpperLevelDigest = new AuthUpperLevelDigest(digest);
						authDigests.add(toweredUpperLevelDigest);
					} else {
//						IAuthDigest authZeroLevelDigest = new AuthZeroLevelDigest<>(tower.key, tower.value, this.toweredTypeUtils);
						IAuthDigest authZeroLevelDigest = new AuthZeroLevelDigest(tower.key, tower.value.getRootDigest(), toweredTypeUtils);
						authDigests.add(authZeroLevelDigest);
					}

					tower = pointedTower;
					level = tower.maxLevel;
				}
			}
		}

		//TODO To achieve faster authentication at the client side can store the (currentTower + nextTower) digest on
		// each node and serve that for the non verifiable results to the client. this will save the client and the
		// server the need to recalculate them at the expense of storing this information in each node.

		return res;
	}

	public ArrayList<IAuthDigest> getAuthDigestsOfFoundKey(KVER version, K key, Tower<KVER, K, V> tower, ArrayList<IAuthDigest> authDigests) throws Exception {
		if (tower.key == null || tower.key.compareTo(key) != 0) {
			authDigests = null;
		} else {
			PartitionSearchRes<K,V,KVER> partitionSearchRes = tower.value.authenticatedSearch(version);
			if (partitionSearchRes == null) {
				authDigests = null;
			} else {
				AuthZeroLevelFoundTowerDigest<KVER,K,V> authZeroLevelFoundTowerDigest = new AuthZeroLevelFoundTowerDigest<>(key, partitionSearchRes,this.toweredTypeUtils);
				authDigests.add(authZeroLevelFoundTowerDigest);
				Tower<KVER, K, V> pointedTower = tower.getPointedTower(0);
				byte[] lastDigest = Tower.getNullableTowerDigest(0, pointedTower);
				AuthLastPointedTowerDigest digestDigest = new AuthLastPointedTowerDigest(lastDigest);
				authDigests.add(digestDigest);
			}
		}
		return authDigests;
	}

	public void rangeSearchInner(int level, KVER version, Tower<KVER, K, V> tower, K keyStart, K keyEnd, ArrayList<IRowDetails<K,V,KVER>> foundRows) throws Exception {
		while (level >= 0) {
			Tower<KVER, K, V> pointedTower = tower.getPointedTower(level);

			if (pointedTower == null) {
				level--;
			} else {
				if (pointedTower.key.compareTo(keyStart) >= 0) {
					if (pointedTower.key.compareTo(keyEnd) <= 0) {
						rangeSearchInner(level - 1, version, tower, keyStart, keyEnd, foundRows);
						V row = pointedTower.value.search(version);
						if (row != null) {
							foundRows.add(row);
						}

						tower = pointedTower;
					} else {
						level--;
					}
				} else {
					tower = pointedTower;
				}
			}
		}
	}

	public void rangeSearchInner(int level, KVER verStart, KVER verEnd, Tower<KVER, K, V> tower, K keyStart, K keyEnd, ArrayList<IRowDetails<K,V,KVER>> foundRows) throws Exception {
		while (level >= 0) {
			Tower<KVER, K, V> pointedTower = tower.getPointedTower(level);

			if (pointedTower == null) {
				level--;
			} else {
				if (pointedTower.key.compareTo(keyStart) >= 0) {
					if (pointedTower.key.compareTo(keyEnd) <= 0) {
						rangeSearchInner(level - 1, verStart, verEnd, tower, keyStart, keyEnd, foundRows);
						pointedTower.value.search(verStart, verEnd, foundRows);

						tower = pointedTower;
					} else {
						level--;
					}
				} else {
					tower = pointedTower;
				}
			}
		}
	}

	public void rangeSearch1(KVER version, K keyStart, K keyEnd, ArrayList<IRowDetails<K,V,KVER>> foundRows) throws Exception {
		Utils.assertNotNull(keyStart, "keyStart should not be null");
		Utils.assertNotNull(keyEnd, "keyEnd should not be null");
		Utils.assertTrue(keyStart.compareTo(keyEnd) <= 0, "keyStart should be <= keyEnd");

		rangeSearchInner(this.head.maxLevel, version, this.head, keyStart, keyEnd, foundRows);

	}

	public void rangeSearch2(KVER verStart, KVER verEnd, K key, ArrayList<IRowDetails<K,V,KVER>> foundRows) throws Exception {
		Utils.assertNotNull(key, "key null");
		Utils.assertTrue(verStart.compareTo(verEnd) <= 0);
		Utils.assertTrue(this.head.maxLevel >= 0, "this.head.maxLevel < 0");

		Tower<KVER,K,V> tower = this.head;
		int level = tower.maxLevel;

		while (level >= 0) {
			Tower<KVER,K,V> pointedTower = tower.getPointedTower(level);

			if (pointedTower == null) {
				level--;
			} else {
				int cmp = pointedTower.key.compareTo(key);
				if (cmp > 0) {
					level--;
				} else if (cmp == 0) {
					pointedTower.value.search(verStart,verEnd,foundRows);
					return;
				} else {
					tower = pointedTower;
				}
			}
		}
	}

	public void rangeSearch3(KVER verStart, KVER verEnd, K keyStart, K keyEnd, ArrayList<IRowDetails<K,V,KVER>> foundRows) throws Exception {
		Utils.assertNotNull(keyStart, "keyStart should not be null");
		Utils.assertNotNull(keyEnd, "keyEnd should not be null");
		Utils.assertTrue(keyStart.compareTo(keyEnd) <= 0, "keyStart should be <= keyEnd");
		Utils.assertTrue(verStart.compareTo(verEnd) <= 0);

		rangeSearchInner(this.head.maxLevel, verStart, verEnd, this.head, keyStart, keyEnd, foundRows);
	}

	public ArrayList<Tower<KVER,K,V>> getSortedItems(boolean includeHead) throws Exception {
		ArrayList<Tower<KVER,K,V>> al = new ArrayList<>();
		Stack<StackTuple<KVER,K,V>> stk = new Stack<>();
		stk.push(new StackTuple<>(0, this.head, null, false));
		while (!stk.isEmpty()) {
			StackTuple<KVER,K,V> curStkTpl = stk.pop();
			Tower<KVER,K,V> tower = curStkTpl.tower;
			if (tower.isHeadTower()) {
				if (includeHead) {
					al.add(tower);
				}
			} else {
				al.add(tower);
			}
			for (int i = tower.levelNodes.size() - 1; i >= 0; i--) {
				Tower<KVER,K,V> t = tower.getPointedTower(i);
				if (t != null) {
					stk.push(new StackTuple<>(i, t, null, false));
				}
			}
		}

		return al;
	}


}
