package approach4.valueDataStructures;

public class Version<KVER extends  Comparable<KVER>> implements Comparable<Version<KVER>>{
    private KVER validFrom;
    private KVER validTo;

    public Version(KVER validFrom, KVER validTo) {
        this.validFrom = validFrom;
        this.validTo = validTo;
    }

    public void setValidFrom(KVER version) throws Exception {
        if (this.validFrom != null) {
            throw new Exception("create version was already set");
        }
        this.validFrom = version;
    }

    public void setValidTo(KVER version) throws Exception {
        if (this.validTo != null) {
            throw new Exception("delete version was already set");
        }
        this.validTo = version;
    }

    public KVER getValidFrom() throws Exception {
        if (this.validFrom == null) {
            throw new Exception("validFrom was not calculated before query");
        }
        return this.validFrom;
    }

    public KVER getValidTo() { return this.validTo; }

    @Override
    public int hashCode() {
        int verStartHash = 0;
        int verEndHash = 0;
        if (this.validTo != null) {
            verEndHash = this.validTo.hashCode();
        }

        int hash = 17;
        hash = hash * 31 + verStartHash;
        hash = hash * 31 + verEndHash;
        return hash;
    }

    @Override
    public boolean equals(Object version){
        if(!(version instanceof Version)){
            return false;
        }
        Version<KVER> v =(Version<KVER>) version;
        return this.compareTo(v) == 0;
    }

    @Override
    public int compareTo(Version<KVER> version) {
        if (this.validFrom != null) {
            if (version.validFrom != null) {
                int cmp = this.validFrom.compareTo(version.validFrom);
                if (cmp != 0) {
                    return cmp;
                } else if (this.validTo != null) {
                    if (version.validTo != null) {
                        return this.validTo.compareTo(version.validTo);
                    } else {
                        return 1;
                    }
                } else if (version.validTo != null) {
                    return -1;
                } else {
                    return 0;
                }
            } else {
                return 1;
            }
        } else if (version.validFrom != null) {
            return -1;
        } else if (this.validTo != null) {
            if (version.validTo != null) {
                return this.validTo.compareTo(version.validTo);
            } else {
                return 1;
            }
        } else if (version.validTo != null) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return "Version{" +
                "validFrom=" + validFrom +
                ", validTo=" + validTo +
                '}';
    }
}
