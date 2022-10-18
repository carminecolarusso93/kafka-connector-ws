package model;

public class KafkaMessage {
    
    public double avgTravelTime;
    public double sdTravelTime;
    public int numVehicles;
    public int aggPeriod;
    public long domainAggTimestamp;
    public long addTimestamp;
    public long linkid;
    public String areaName;
    
    public KafkaMessage(double avgTravelTime, double sdTravelTime, int numVehicles, int aggPeriod,
            long domainAggTimestamp, long addTimestamp, long linkid, String areaName) {
        this.avgTravelTime = avgTravelTime;
        this.sdTravelTime = sdTravelTime;
        this.numVehicles = numVehicles;
        this.aggPeriod = aggPeriod;
        this.domainAggTimestamp = domainAggTimestamp;
        this.addTimestamp = addTimestamp;
        this.linkid = linkid;
        this.areaName = areaName;
    }

    public double getAvgTravelTime() {
        return avgTravelTime;
    }

    public void setAvgTravelTime(double avgTravelTime) {
        this.avgTravelTime = avgTravelTime;
    }

    public double getSdTravelTime() {
        return sdTravelTime;
    }

    public void setSdTravelTime(double sdTravelTime) {
        this.sdTravelTime = sdTravelTime;
    }

    public int getNumVehicles() {
        return numVehicles;
    }

    public void setNumVehicles(int numVehicles) {
        this.numVehicles = numVehicles;
    }

    public int getAggPeriod() {
        return aggPeriod;
    }

    public void setAggPeriod(int aggPeriod) {
        this.aggPeriod = aggPeriod;
    }

    public long getDomainAggTimestamp() {
        return domainAggTimestamp;
    }

    public void setDomainAggTimestamp(long domainAggTimestamp) {
        this.domainAggTimestamp = domainAggTimestamp;
    }

    public long getAddTimestamp() {
        return addTimestamp;
    }

    public void setAddTimestamp(long addTimestamp) {
        this.addTimestamp = addTimestamp;
    }

    public long getLinkid() {
        return linkid;
    }

    public void setLinkid(long linkid) {
        this.linkid = linkid;
    }

    public String getAreaName() {
        return areaName;
    }

    public void setAreaName(String areaName) {
        this.areaName = areaName;
    }

    @Override
    public String toString() {
        return "KafkaMessage [addTimestamp=" + addTimestamp + ", aggPeriod=" + aggPeriod + ", areaName=" + areaName
                + ", avgTravelTime=" + avgTravelTime + ", domainAggTimestamp=" + domainAggTimestamp + ", linkid="
                + linkid + ", numVehicles=" + numVehicles + ", sdTravelTime=" + sdTravelTime + "]";
    }

    



}
