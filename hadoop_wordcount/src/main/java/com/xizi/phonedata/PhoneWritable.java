package com.xizi.phonedata;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PhoneWritable implements WritableComparable<PhoneWritable> {
    private Integer uploads;
    private Integer downloads;
    private Integer totals;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PhoneWritable that = (PhoneWritable) o;
        return Objects.equals(uploads, that.uploads) &&
                Objects.equals(downloads, that.downloads) &&
                Objects.equals(totals, that.totals);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uploads, downloads, totals);
    }

    //排序
    @Override
    public int compareTo(PhoneWritable o) {
        return this.totals-o.totals;
    }


        //对读入文件进行序列化 write和read顺序必须严格一致
        @Override
        public void readFields(DataInput in) throws IOException {
            this.downloads=in.readInt();
            this.uploads=in.readInt();
            this.totals=in.readInt();
        }

        //写出的序列化
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(downloads);
            out.writeInt(uploads);
            out.writeInt(totals);
        }

    public PhoneWritable() {
    }

    public PhoneWritable(Integer uploads, Integer downloads, Integer totals) {
        this.uploads = uploads;
        this.downloads = downloads;
        this.totals = totals;
    }

    public Integer getUploads() {
        return uploads;
    }

    public void setUploads(Integer uploads) {
        this.uploads = uploads;
    }

    public Integer getDownloads() {
        return downloads;
    }

    public void setDownloads(Integer downloads) {
        this.downloads = downloads;
    }

    public Integer getTotals() {
        return totals;
    }

    public void setTotals(Integer totals) {
        this.totals = totals;
    }

    @Override
    public String toString() {
        return uploads+"\t"+downloads+"\t"+totals;
    }
}
