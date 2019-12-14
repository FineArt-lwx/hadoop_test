package com.lwx.domain;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Version implements WritableComparable<Version> {

    private String id;
    private String name;
    private String game;
    private Integer hour;
    private String source;
    private String version;
    private String city;

    public Version(String id, String name, String game, Integer hour, String source, String version, String city) {
        super();
        this.id = id;
        this.name = name;
        this.game = game;
        this.hour = hour;
        this.source = source;
        this.version = version;
        this.city = city;
    }

    public Version() {
        super();
        // TODO Auto-generated constructor stub
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGame() {
        return game;
    }

    public void setGame(String game) {
        this.game = game;
    }

    public Integer getHour() {
        return hour;
    }

    public void setHour(Integer hour) {
        this.hour = hour;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeUTF(id);
        out.writeUTF(name);
        out.writeUTF(game);
        out.writeInt(hour);
        out.writeUTF(source);
        out.writeUTF(version);
        out.writeUTF(city);
    }

    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        this.id = in.readUTF();
        this.name = in.readUTF();
        this.game = in.readUTF();
        this.hour = in.readInt();
        this.source = in.readUTF();
        this.version = in.readUTF();
        this.city = in.readUTF();
    }

    public int compareTo(Version version) {

        int resultID = this.id.compareTo(version.getId());
        if (resultID == 0) {
            int resultName = this.name.compareTo(version.getName());
            if (resultName == 0) {
                return this.version.compareTo(version.getVersion());
            } else {
                return resultName;
            }
        } else {
            return resultID;
        }
    }

    @Override
    public String toString() {
        return id + "," + name + "," + game + "," + hour + "," + source + "," + version + "," + city;
    }

}
