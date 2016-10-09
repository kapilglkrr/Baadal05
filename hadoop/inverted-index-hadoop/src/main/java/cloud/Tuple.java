package cloud;

import java.lang.reflect.*;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Tuple<F extends Writable, S extends Writable> implements Writable {
    private Class<? extends Writable> firstClass;
    private Class<? extends Writable> secondClass;
    private F first;
    private S second;

    public Tuple(final Class<? extends Writable> firstClass, final Class<? extends Writable> secondClass) {
        this.firstClass = firstClass;
        this.secondClass = secondClass;
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

	public Class<? extends Writable> getFirstClass() {
		return firstClass;
	}

	public void setFirstClass(Class<? extends Writable> firstClass) {
		this.firstClass = firstClass;
	}

	public Class<? extends Writable> getSecondClass() {
		return secondClass;
	}

	public void setSecondClass(Class<? extends Writable> secondClass) {
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


