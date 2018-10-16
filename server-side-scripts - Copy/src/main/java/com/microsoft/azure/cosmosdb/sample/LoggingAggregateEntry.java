
package com.microsoft.azure.cosmosdb.sample;

public class LoggingAggregateEntry
{
    private String Id;
    private Boolean IsMetadata;
    private Integer MinSize;
    private Integer MaxSize;
    private Integer TotalSize;
    private String DeviceId;

    public String getId() { return this.Id; }
    public void setId( String Id ) { this.Id = Id; }

    public Boolean getIsMetadata() { return this.IsMetadata; }
    public void setIsMetadata( Boolean IsMetadata ) { this.IsMetadata = IsMetadata; }

    public Integer getMinSize() { return this.MinSize; }
    public void setMinSize( Integer MinSize ) { this.MinSize = MinSize; }

    public Integer getMaxSize() { return this.MaxSize; }
    public void setMaxSize( Integer MaxSize ) { this.MinSize = MaxSize; }

    public Integer getTotalSize() { return this.TotalSize; }
    public void setTotalSize( Integer TotalSize ) { this.MinSize = TotalSize; }

    public String getDeviceId() { return this.DeviceId; }
    public void setDeviceId( String DeviceId ) { this.DeviceId = DeviceId; }
}