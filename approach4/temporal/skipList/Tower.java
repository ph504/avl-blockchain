package approach4.temporal.skipList;

import approach4.IRowDetails;
import approach4.Utils;
import approach4.temporal.temporalPartitions.Partitions;

import java.util.ArrayList;

public class Tower<KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K, V, KVER>> {

	public int maxLevel;
	protected ArrayList<Tower<KVER,K,V>> levelNodes;
	protected ArrayList<byte[]> digests;

	public final TowerType towerType;

	public final K key;

	public Partitions<K,V,KVER> value;

	public final int partitionCapacity;


	public enum TowerType {
		HEAD,
		REGULAR
	}

	public Tower(KVER version, V row, int maxLevel, int partitionCapacity) throws Exception {
		this(TowerType.REGULAR, version, row, maxLevel, partitionCapacity);
	}

	public Tower(TowerType towerType, KVER version, V row, int maxLevel, int partitionCapacity) throws Exception {
//		checkIsValidLevelSize(maxLevel);
		K key = null;
		if (towerType == TowerType.REGULAR) {
			key = row.getKey();
		}
//		K key = null;
//		if (towerType == TowerType.Head) {
//			Utils.assertNull(row, "row must be null in head node");
//		}
//		else if (towerType == TowerType.REGULAR) {
//			Utils.assertNotNull(row, "row can't be null in non head node");
//			key = row.getKey();
//		} else {
//			throw new Exception("should not get here!!!");
//		}

		this.towerType = towerType;
		this.key = key;
		this.partitionCapacity = partitionCapacity;
		this.value = new Partitions<>(this.partitionCapacity);
		if (row != null) {
			this.value.add(version, row);
		}


		this.maxLevel = maxLevel;
		// TODO when creating a new Tower look at providing initial levels as MAX_LEVEL/2
		this.levelNodes = new ArrayList<>(maxLevel + 1);
		this.digests = new ArrayList<>(maxLevel + 1);

		for (int i = 0; i <= maxLevel; i++) {
			this.levelNodes.add(null);
			this.digests.add(null);
		}
	}



	public void processLevelDigest(int level, Tower<KVER,K,V> pointedTower, ToweredTypeUtils<K,V> toweredTypeUtils) throws Exception {
//		checkLevelInRange(level);

		byte[] digest = null;
		if (level == 0) {
			digest = getZeroLevelDigest(this, pointedTower, toweredTypeUtils);
		} else {
			digest = getUpperLevelDigest(level, pointedTower);
		}

		this.digests.set(level, digest);
	}

	public byte[] getUpperLevelDigest(int level, Tower<KVER,K,V> pointedTower) throws Exception {
//		Utils.assertTrue(level > 0, "level must be bigger than 0");
		byte[] pointedTowerDigest = getNullableTowerDigest(level, pointedTower);
		byte[] bottomDigest = this.getBottomLevelDigest(level);
		return Utils.getHash(bottomDigest, pointedTowerDigest);
	}

	public static <KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K,V, KVER>>
	byte[] getZeroLevelDigest(Tower<KVER,K,V> tower, Tower<KVER,K,V> pointedTower, ToweredTypeUtils<K,V> toweredTypeUtils) throws Exception {
		byte[] curTowerDigest = getKeyValueDigest(tower.key, tower.value, toweredTypeUtils);

//		Utils.assertNotNull(curTowerDigest,"digest should not be null");
		byte[] pointedTowerDigest = getNullableTowerDigest(0, pointedTower);
		return Utils.getHash(curTowerDigest, pointedTowerDigest);
	}

//	public static <KVER extends Comparable<KVER>,K extends Comparable<K>,V>
//	byte[] getZeroLevelDigest(Tower<K, V> tower, Tower<K, V> pointedTower, ToweredTypeUtils<K, V> toweredTypeUtils) throws Exception {
//		byte[] curTowerKeyValueDigest = getKeyValueDigest(tower.key, tower.value, toweredTypeUtils);
//		K nextTowerKey = null;
//		V nextTowerValue = null;
//		// TODO replace with nullDigest instead of digest of null values
//		if (pointedTower != null) {
//			nextTowerKey = pointedTower.key;
//			nextTowerValue = pointedTower.value;
//		}
//		byte[] nextTowerKeyValueDigest = getKeyValueDigest(nextTowerKey, nextTowerValue, toweredTypeUtils);
//		byte[] currentAndNextTowersDigest = Utils.commutativeHash(curTowerKeyValueDigest, nextTowerKeyValueDigest);
//		byte[] pointedTowerDigest = getNullableTowerDigest(0, pointedTower);
//		byte[] currentNextAndPointedTowersDigest = Utils.commutativeHash(currentAndNextTowersDigest, pointedTowerDigest);
//		return currentNextAndPointedTowersDigest;
//	}

	public static <KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K, V, KVER>>
	byte[] getKeyValueDigest(K curTowerKey, Partitions<K,V,KVER> curTowerValue, ToweredTypeUtils<K,V> toweredTypeUtils) throws Exception {
		byte[] curTowerKeyDigest = Utils.getNullableObjectHash(curTowerKey, toweredTypeUtils.kTypeUtils);
		byte[] curTowerValueDigest = curTowerValue.getRootDigest();
		byte[] digest = Utils.getHash(curTowerKeyDigest, curTowerValueDigest);
		return digest;
	}

	public static <KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K,V,KVER>>
	byte[] getNullableTowerDigest(int level, Tower<KVER,K,V> tower) throws Exception {
		byte[] digest;
		if (tower == null) {
			digest = Utils.nullDigest;
		} else {
			digest = tower.digests.get(level);
//			Utils.assertNotNull(digest,"digest should not be null");
		}
		return digest;
	}

	public byte[] getBottomLevelDigest(int level) throws Exception {
//		checkLevelInRange(level);
//		Utils.assertTrue(level > 0, "level <= 0");

		return this.digests.get(level - 1);
	}

	public Tower<KVER,K,V> getPointedTower(int level) throws Exception {
//		checkLevelInRange(level);
		return this.levelNodes.get(level);
	}

	public void setPointedTower(int level, Tower<KVER,K,V> tower) throws Exception {
//		checkLevelInRange(level);
		this.levelNodes.set(level,tower);
	}

	public byte[] getDigest(int level) throws Exception {
//		checkLevelInRange(level);
		return this.digests.get(level);
	}

	public void extendHeadTowerMaxLevel(int newMaxLevel, ToweredTypeUtils<K,V> toweredTypeUtils) throws Exception {
//		checkIsValidLevelSize(newMaxLevel);
//		Utils.assertTrue(this.towerType == TowerType.Head, "can only extend max level of head tower");

		if (this.maxLevel < newMaxLevel) {
			// TODO when extending a header tower's levels look at extending by increments of MAX_LEVEL/2
			ArrayList<Tower<KVER,K,V>> extendedLevelNodes = new ArrayList<>(newMaxLevel + 1);
			ArrayList<byte[]> extendedDigests = new ArrayList<>(newMaxLevel + 1);

			int i = 0;
			while (i <= this.maxLevel) {
				extendedLevelNodes.add(this.levelNodes.get(i));
				extendedDigests.add(this.digests.get(i));
				i++;
			}

			this.maxLevel = newMaxLevel;
			this.levelNodes = extendedLevelNodes;
			this.digests = extendedDigests;

			while (i <= newMaxLevel) {
				this.levelNodes.add(null);
				this.digests.add(null);
				processLevelDigest(i, null, toweredTypeUtils);
				i++;
			}
		}
	}

	private void checkLevelInRange(int level) throws Exception {
		checkLevelInRange(this, level);
	}

	public static <KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K,V,KVER>>
	void checkLevelInRange(Tower<KVER,K,V> tower, int level) throws Exception {
		if (level > tower.maxLevel || level < 0) {
			throw new Exception("invalid level");
		}
	}

	private static void checkIsValidLevelSize(int level) throws Exception {
		if (level > ToweredSkipList.MAX_LEVEL || level < 0) {
			throw new Exception("invalid level");
		}
	}

	public boolean isHeadTower() throws Exception {
		return this.towerType == Tower.TowerType.HEAD;
	}

	@Override
	public String toString() {
		return "key: " + this.key + " maxLevel: " + this.maxLevel + " value: (" + this.value + ")";
	}




}
