package site.nemo.learn;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TrafficEntity implements Writable {

    private Long uploadTraffic;
    private Long downloadTraffic;
    private Long totalTraffic;

    public TrafficEntity() {

    }

    public TrafficEntity(Long uploadTraffic, Long downloadTraffic, Long totalTraffic) {
        this.uploadTraffic = uploadTraffic;
        this.downloadTraffic = downloadTraffic;
        this.totalTraffic = totalTraffic;
    }

    public Long getUploadTraffic() {
        return uploadTraffic;
    }

    public void setUploadTraffic(Long uploadTraffic) {
        this.uploadTraffic = uploadTraffic;
    }

    public Long getDownloadTraffic() {
        return downloadTraffic;
    }

    public void setDownloadTraffic(Long downloadTraffic) {
        this.downloadTraffic = downloadTraffic;
    }

    public Long getTotalTraffic() {
        return totalTraffic;
    }

    public void setTotalTraffic(Long totalTraffic) {
        this.totalTraffic = totalTraffic;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(uploadTraffic);
        dataOutput.writeLong(downloadTraffic);
        dataOutput.writeLong(totalTraffic);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.uploadTraffic = dataInput.readLong();
        this.downloadTraffic = dataInput.readLong();
        this.totalTraffic = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "" + uploadTraffic +
                "\t" + downloadTraffic +
                "\t" + totalTraffic;
    }
}
