/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.vineyard.impl;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.Type;
import io.v6d.modules.basic.columnar.ColumnarData;
import lombok.val;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.util.Text;

import static java.util.Objects.requireNonNull;

/**
 * A visitor for arrow arrays.
 */
public class PageColumnSource
{
    private final ColumnarData column;
    private final PageColumnMover mover;

    public PageColumnSource(final ColumnarData column)
    {
        this.column = requireNonNull(column, "column is null");
        val vector = column.getVector();
        if (vector instanceof BitVector) {
            mover = new BooleanMover((BitVector) vector);
        }
        else if (vector instanceof TinyIntVector) {
            mover = new ByteMover((TinyIntVector) vector);
        }
        else if (vector instanceof SmallIntVector) {
            mover = new ShortMover((SmallIntVector) vector);
        }
        else if (vector instanceof IntVector) {
            mover = new IntMover((IntVector) vector);
        }
        else if (vector instanceof BigIntVector) {
            mover = new LongMover((BigIntVector) vector);
        }
        else if (vector instanceof Float4Vector) {
            mover = new FloatMover((Float4Vector) vector);
        }
        else if (vector instanceof Float8Vector) {
            mover = new DoubleMover((Float8Vector) vector);
        }
        else if (vector instanceof DecimalVector) {
            mover = new DecimalMover((DecimalVector) vector);
        }
        else if (vector instanceof LargeVarCharVector) {
            mover = new LargeStringMover((LargeVarCharVector) vector);
        }
        else if (vector instanceof VarCharVector) {
            mover = new StringMover((VarCharVector) vector);
        }
        else if (vector instanceof VarBinaryVector) {
            mover = new BinaryMover((VarBinaryVector) vector);
        }
        else if (vector instanceof LargeVarBinaryVector) {
            mover = new LargeBinaryMover((LargeVarBinaryVector) vector);
        }
        else if (vector instanceof DateDayVector) {
            mover = new DateMover((DateDayVector) vector);
        }
        else if (vector instanceof TimeStampMicroTZVector) {
            mover = new TimestampMover((TimeStampMicroTZVector) vector);
        }
        else if (vector instanceof TimeStampMicroVector) {
            mover = new TimestampNTZMover((TimeStampMicroVector) vector);
        }
        else if (vector instanceof NullVector) {
            mover = new NullMover((NullVector) vector);
        }
        else if (vector instanceof IntervalYearVector) {
            mover = new IntervalYearMover((IntervalYearVector) vector);
        }
        else if (vector instanceof IntervalDayVector) {
            mover = new IntervalDayMover((IntervalDayVector) vector);
        }
        else {
            throw new UnsupportedOperationException(
                    "array type is not supported yet: " + vector.getClass());
        }
    }

    public void move(final Block block, final Type type)
    {
        mover.move(block, type);
    }

    public void close()
    {
        ;
    }

    private abstract static class PageColumnMover
    {
        private final ValueVector vector;

        public PageColumnMover(ValueVector vector)
        {
            this.vector = vector;
        }

        public void move(final Block block, final Type type)
        {
            assert block.getPositionCount() == vector.getValueCount();
            for (int index = 0; index < block.getPositionCount(); ++index) {
                move(block, type, index);
            }
        }

        public abstract void move(final Block block, final Type type, int index);
    }

    private static class BooleanMover
            extends PageColumnMover
    {

        private final BitVector accessor;

        BooleanMover(BitVector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
            this.accessor.set(index, type.getBoolean(block, index) ? 1 : 0);
        }
    }

    private static class ByteMover
            extends PageColumnMover
    {

        private final TinyIntVector accessor;

        ByteMover(TinyIntVector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
            this.accessor.set(index, block.getByte(index));
        }
    }

    private static class ShortMover
            extends PageColumnMover
    {

        private final SmallIntVector accessor;

        ShortMover(SmallIntVector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
            this.accessor.set(index, block.getShort(index));
        }
    }

    private static class IntMover
            extends PageColumnMover
    {

        private final IntVector accessor;

        IntMover(IntVector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
            this.accessor.set(index, block.getInt(index));
        }
    }

    private static class LongMover
            extends PageColumnMover
    {

        private final BigIntVector accessor;

        LongMover(BigIntVector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
            this.accessor.set(index, block.getLong(index));
        }
    }

    private static class FloatMover
            extends PageColumnMover
    {

        private final Float4Vector accessor;

        FloatMover(Float4Vector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
            this.accessor.set(index, (float) type.getDouble(block, index));
        }
    }

    private static class DoubleMover
            extends PageColumnMover
    {

        private final Float8Vector accessor;

        DoubleMover(Float8Vector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
            this.accessor.set(index, type.getDouble(block, index));
        }
    }

    private static class DecimalMover
            extends PageColumnMover
    {

        private final DecimalVector accessor;

        DecimalMover(DecimalVector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
            this.accessor.set(index, Decimals.readBigDecimal((DecimalType) type, block, index));
        }
    }

    private static class StringMover
            extends PageColumnMover
    {

        private final VarCharVector accessor;
        private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

        StringMover(VarCharVector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
            this.accessor.set(index, new Text(type.getSlice(block, index).getBytes()));
        }
    }

    private static class LargeStringMover
            extends PageColumnMover
    {

        private final LargeVarCharVector accessor;
        private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

        LargeStringMover(LargeVarCharVector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
            this.accessor.set(index, new Text(type.getSlice(block, index).getBytes()));
        }
    }

    private static class BinaryMover
            extends PageColumnMover
    {

        private final VarBinaryVector accessor;

        BinaryMover(VarBinaryVector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
            this.accessor.set(index, type.getSlice(block, index).getBytes());
        }
    }

    private static class LargeBinaryMover
            extends PageColumnMover
    {

        private final LargeVarBinaryVector accessor;

        LargeBinaryMover(LargeVarBinaryVector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
            this.accessor.set(index, type.getSlice(block, index).getBytes());
        }
    }

    private static class DateMover
            extends PageColumnMover
    {

        private final DateDayVector accessor;

        DateMover(DateDayVector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
        }
    }

    private static class TimestampMover
            extends PageColumnMover
    {

        private final TimeStampMicroTZVector accessor;

        TimestampMover(TimeStampMicroTZVector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
        }
    }

    private static class TimestampNTZMover
            extends PageColumnMover
    {

        private final TimeStampMicroVector accessor;

        TimestampNTZMover(TimeStampMicroVector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
        }
    }

    private static class NullMover
            extends PageColumnMover
    {
        NullMover(NullVector vector)
        {
            super(vector);
        }

        @Override
        public void move(final Block block, final Type type) {}

        @Override
        public void move(final Block block, final Type type, int index)
        {
        }
    }

    private static class IntervalYearMover
            extends PageColumnMover
    {

        private final IntervalYearVector accessor;

        IntervalYearMover(IntervalYearVector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
        }
    }

    private static class IntervalDayMover
            extends PageColumnMover
    {

        private final IntervalDayVector accessor;
        private final NullableIntervalDayHolder intervalDayHolder = new NullableIntervalDayHolder();

        IntervalDayMover(IntervalDayVector vector)
        {
            super(vector);
            this.accessor = vector;
        }

        @Override
        public void move(final Block block, final Type type, int index)
        {
        }
    }

    // refer from spark:
    // https://github.com/apache/spark/blob/master/common/unsafe/src/main/java/org/apache/spark/sql/catalyst/util/DateTimeConstants.java
    private class DateTimeConstants
    {
        public static final int MONTHS_PER_YEAR = 12;

        public static final byte DAYS_PER_WEEK = 7;

        public static final long HOURS_PER_DAY = 24L;

        public static final long MINUTES_PER_HOUR = 60L;

        public static final long SECONDS_PER_MINUTE = 60L;
        public static final long SECONDS_PER_HOUR = MINUTES_PER_HOUR * SECONDS_PER_MINUTE;
        public static final long SECONDS_PER_DAY = HOURS_PER_DAY * SECONDS_PER_HOUR;

        public static final long MILLIS_PER_SECOND = 1000L;
        public static final long MILLIS_PER_MINUTE = SECONDS_PER_MINUTE * MILLIS_PER_SECOND;
        public static final long MILLIS_PER_HOUR = MINUTES_PER_HOUR * MILLIS_PER_MINUTE;
        public static final long MILLIS_PER_DAY = HOURS_PER_DAY * MILLIS_PER_HOUR;

        public static final long MICROS_PER_MILLIS = 1000L;
        public static final long MICROS_PER_SECOND = MILLIS_PER_SECOND * MICROS_PER_MILLIS;
        public static final long MICROS_PER_MINUTE = SECONDS_PER_MINUTE * MICROS_PER_SECOND;
        public static final long MICROS_PER_HOUR = MINUTES_PER_HOUR * MICROS_PER_MINUTE;
        public static final long MICROS_PER_DAY = HOURS_PER_DAY * MICROS_PER_HOUR;

        public static final long NANOS_PER_MICROS = 1000L;
        public static final long NANOS_PER_MILLIS = MICROS_PER_MILLIS * NANOS_PER_MICROS;
        public static final long NANOS_PER_SECOND = MILLIS_PER_SECOND * NANOS_PER_MILLIS;
    }
}

