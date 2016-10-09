package cloud;

import java.lang.reflect.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableFactories;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TupleC<F extends WritableComparable, S extends WritableComparable>  implements WritableComparable {
    public TupleC(Class<? extends WritableComparable> firstClass,
			Class<? extends WritableComparable> secondClass, F first, S second) {
		super();
		this.firstClass = firstClass;
		this.secondClass = secondClass;
		this.first = first;
		this.second = second;
	}

	public TupleC(F first, S second) {
		super();
		this.first = first;
		this.second = second;
	}

	private Class<? extends WritableComparable> firstClass;
    private Class<? extends WritableComparable> secondClass;
    private F first;
    private S second;

    public TupleC(final Class<? extends WritableComparable> firstClass, final Class<? extends WritableComparable> secondClass) {
        this.firstClass = firstClass;
        this.secondClass = secondClass;
    }

    public int compareTo(Object object) {
        TupleC ip2 = (TupleC) object;
        int cmp = getFirst().compareTo(ip2.getFirst());
        if (cmp != 0)
            return cmp;
        return getSecond().compareTo(ip2.getSecond()); // reverse
    }
    public void readFields(DataInput in) throws IOException {

        try {
            if (first == null)
                first = (F)firstClass.newInstance();
            if (second == null)
                second = (S)secondClass.newInstance();
        } catch (java.lang.InstantiationException e) {
            System.out.println("nothing");
        } catch (java.lang.IllegalAccessException e) {
            System.out.println("nothing");
        }
        first.readFields(in);
        second.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    public java.lang.String toString() {
        return this.getClass().getSimpleName() + "(" + this.getFirst() + ", " + this.getSecond() + ")";
    }

	public Class<? extends WritableComparable> getFirstClass() {
		return firstClass;
	}

	public void setFirstClass(Class<? extends WritableComparable> firstClass) {
		this.firstClass = firstClass;
	}

	public Class<? extends WritableComparable> getSecondClass() {
		return secondClass;
	}

	public void setSecondClass(Class<? extends WritableComparable> secondClass) {
		this.secondClass = secondClass;
	}

	public F getFirst() {
		return first;
	}

	public void setFirst(F first) {
		this.first = first;
	}

	public S getSecond() {
		return second;
	}

	public void setSecond(S second) {
		this.second = second;
	}
}


