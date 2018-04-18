package cn.willon.bigdata.grms.s8_SaveRecommandResult2DB;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ResultKey implements WritableComparable, DBWritable {

    private String uid;
    private String gid;
    private int exp;

    public void readFields(ResultSet rs) throws SQLException {
        uid = rs.getString("uid");
        gid = rs.getString("gid");
        exp = rs.getInt("exp");
    }


    public void write(PreparedStatement ps) throws SQLException {
        ps.setString(1, uid);
        ps.setString(2,gid);
        ps.setInt(3, exp);
    }

    public int compareTo(Object o) {
        int res = -1;
        if (o instanceof ResultKey){
            ResultKey r = (ResultKey) o;
            //比较 uid
            int  a = Integer.parseInt(this.getUid());
            int  b = Integer.parseInt(r.getUid());
            res = a > b ? 1 : (a == b) ? 0 : -1;

        }
        return res;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ResultKey)) return false;

        ResultKey resultKey = (ResultKey) o;

        if (exp != resultKey.exp) return false;
        if (!uid.equals(resultKey.uid)) return false;
        return gid.equals(resultKey.gid);
    }

    @Override
    public int hashCode() {
        int result = uid.hashCode();
        result = 31 * result + gid.hashCode();
        result = 31 * result + exp;
        return result;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(uid);
        out.writeUTF(gid);
        out.writeInt(exp);
    }

    public void readFields(DataInput in) throws IOException {
        this.uid = in.readUTF();
        this.gid = in.readUTF();
        this.exp = in.readInt();
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getGid() {
        return gid;
    }

    public void setGid(String gid) {
        this.gid = gid;
    }

    public int getExp() {
        return exp;
    }

    public void setExp(int exp) {
        this.exp = exp;
    }
}
