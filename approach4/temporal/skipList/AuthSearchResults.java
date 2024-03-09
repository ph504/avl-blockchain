package approach4.temporal.skipList;



import approach4.IAuthDigest;
import approach4.IRowDetails;
import approach4.Utils;

import java.util.ArrayList;

public class AuthSearchResults<KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K,V,KVER>> {
    public final ArrayList<IAuthDigest> authTowerDigests;

    public AuthSearchResults(ArrayList<IAuthDigest> authDigests) {
        this.authTowerDigests = authDigests;
    }

    public V validateAndGetRow(KVER version, K searchKey, byte[] rootDigest) throws Exception {
        byte[] rollingDigest = getDigest(this.authTowerDigests);
        if (Utils.byteArrayCompareTo(rollingDigest, rootDigest) != 0) {
            throw new Exception("proof doesn't match on root digest!");
        }
        return this.validateAndGetRow(version, searchKey);
    }

    public static <KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K,V,KVER>>
    byte[] getDigest(ArrayList<IAuthDigest> authTowerDigests) throws Exception {
        int lastItemPos = authTowerDigests.size() - 1;

        AuthLastPointedTowerDigest pointedTowerAuthDigest = (AuthLastPointedTowerDigest) authTowerDigests.get(lastItemPos);
        byte[] pointedTowerDigest = pointedTowerAuthDigest.getZeroLevelDigest();
        lastItemPos--;

        AuthZeroLevelFoundTowerDigest<KVER,K,V> authZeroLevelFoundTowerDigest = (AuthZeroLevelFoundTowerDigest<KVER,K,V>) authTowerDigests.get(lastItemPos);
        lastItemPos--;

        byte[] rollingDigest = authZeroLevelFoundTowerDigest.getZeroLevelDigest(pointedTowerDigest);
        rollingDigest = getRollingDigest(authTowerDigests, lastItemPos, rollingDigest);
        return rollingDigest;
    }

    public static <KVER extends Comparable<KVER>,K extends Comparable<K>,V extends IRowDetails<K,V,KVER>>
    byte[] getRollingDigest(ArrayList<IAuthDigest> authTowerDigests, int lastItemPos, byte[] rollingDigest) throws Exception {
        while (lastItemPos >= 0) {
            IAuthDigest authDigest = authTowerDigests.get(lastItemPos);

            if (authDigest instanceof AuthZeroLevelDigest) {
                AuthZeroLevelDigest towerZeroLevelAuthDigest = (AuthZeroLevelDigest) authDigest;
                rollingDigest = towerZeroLevelAuthDigest.getZeroLevelDigest(rollingDigest);
            } else if (authDigest instanceof AuthUpperLevelDigest) {
                AuthUpperLevelDigest toweredUpperLevelDigest = (AuthUpperLevelDigest) authDigest;
                rollingDigest = toweredUpperLevelDigest.getUpperLevelDigest(rollingDigest);
            } else {
                throw new Exception("should not get here");
            }

            lastItemPos--;
        }
        return rollingDigest;
    }

//    public static <K extends Comparable<K>, V>
//    TupleTwo<byte[], Integer> getRollingDigest(ArrayList<IAuthDigest> authTowerDigests, int lastItemPos, AuthZeroLevelDigest<KVER,K, V> nextTowerZeroLevelAuthDigest, byte[] rollingDigest) throws Exception {
//        while (lastItemPos >= 0) {
//            IAuthDigest authDigest = authTowerDigests.get(lastItemPos);
//
//            if (authDigest instanceof AuthZeroLevelDigest) {
//                AuthZeroLevelDigest<KVER,K, V> towerZeroLevelAuthDigest = (AuthZeroLevelDigest<KVER,K, V>) authDigest;
//                rollingDigest = towerZeroLevelAuthDigest.getZeroLevelDigest(nextTowerZeroLevelAuthDigest, rollingDigest);
//                nextTowerZeroLevelAuthDigest = towerZeroLevelAuthDigest;
//            } else if (authDigest instanceof AuthUpperLevelDigest) {
//                AuthUpperLevelDigest toweredUpperLevelDigest = (AuthUpperLevelDigest) authDigest;
//                rollingDigest = toweredUpperLevelDigest.getUpperLevelDigest(rollingDigest);
//            } else {
//                break;
//            }
//
//            lastItemPos--;
//        }
//        return new TupleTwo<>(rollingDigest, lastItemPos);
//    }

    public V validateAndGetRow(KVER version, K searchKey) throws Exception {
        int lastItemPos = this.authTowerDigests.size() - 1;

        // bypass last digest item
        lastItemPos--;

        AuthZeroLevelFoundTowerDigest<KVER, K, V> partitionSearchRes = (AuthZeroLevelFoundTowerDigest<KVER, K, V>) authTowerDigests.get(lastItemPos);
        V elem = partitionSearchRes.getElem(version);
        Utils.assertTrue(elem.getVersion().getValidFrom().compareTo(version) <= 0, "should be true");
        Utils.assertTrue(elem.getKey().compareTo(searchKey) == 0, "should be true");

        return elem;
    }

}
