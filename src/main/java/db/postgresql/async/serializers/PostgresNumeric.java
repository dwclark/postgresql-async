package db.postgresql.async.serializers;

import java.nio.ByteBuffer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.math.MathContext;
import java.util.Arrays;

class PostgresNumeric {
    private static final BigInteger MOD = BigInteger.valueOf(10_000);
    private static final short POSITIVE = 0x0000;
    private static final short NEGATIVE = 0x4000;
    private static final int NBASE_DIGITS = 4;
    
    private final short numberDigits;
    private final short weight;
    private final short sign;
    private final short displayScale;
    private final short[] digits;

    public int getDigitsBefore() { return weight + 1; }
    public int getDigitsAfter() { return digits.length - getDigitsBefore(); }

    public static short sign(final BigDecimal bd) {
        return bd.signum() < 0 ? NEGATIVE : POSITIVE;
    }

    public static int nbaseDigits(final int decimalDigits) {
        if(decimalDigits < NBASE_DIGITS) {
            return 1;
        }
        else {
            return (decimalDigits / NBASE_DIGITS) + (decimalDigits % NBASE_DIGITS);
        }
    }

    public static void toNbase(final BigInteger bi, final short[] digits, final int left, final int size) {
        BigInteger soFar = bi;
        for(int i = (left + size) - 1; i >= left; i--) {
            BigInteger[] result = soFar.divideAndRemainder(MOD);
            soFar = result[0];
            digits[i] = result[1].shortValue();
        }
    }

    public static BigInteger fromNbase(final short[] ary, final int left, final int size) {
        BigInteger power = BigInteger.ONE;
        BigInteger total = BigInteger.ZERO;
        for(int i = (left + size) - 1; i >= left; i--) {
            total = total.add(BigInteger.valueOf(ary[i]).multiply(power));
            power = power.multiply(MOD);
        }

        return total;
    }

    public PostgresNumeric(final ByteBuffer buffer) {
        this.numberDigits = buffer.getShort();
        this.weight = buffer.getShort();
        this.sign = buffer.getShort();
        this.displayScale = buffer.getShort();
        this.digits = new short[numberDigits];
        for(int i = 0; i < digits.length; ++i) {
            digits[i] = buffer.getShort();
        }
    }

    public PostgresNumeric(final BigDecimal bd) {
        this.sign = sign(bd);
        final BigDecimal val = bd.abs().stripTrailingZeros();
        final int totalDigits = val.precision();
        final int afterDecimal = val.scale();
        final int beforeDecimal = totalDigits - afterDecimal;
        final int nbaseDigitsBefore = nbaseDigits(beforeDecimal);
        final int nbaseDigitsAfter = nbaseDigits(afterDecimal);

        this.weight = (short) (nbaseDigitsBefore - 1);
        this.displayScale = (short) afterDecimal;
        this.numberDigits = (short) (nbaseDigitsBefore + nbaseDigitsAfter);
        this.digits = new short[nbaseDigitsBefore + nbaseDigitsAfter];

        final BigDecimal before = bd.round(new MathContext(beforeDecimal, RoundingMode.DOWN));
        final BigDecimal after = bd.subtract(before).movePointRight(nbaseDigitsAfter * NBASE_DIGITS);
        toNbase(before.toBigInteger(), digits, 0, nbaseDigitsBefore);
        toNbase(after.toBigInteger(), digits, nbaseDigitsBefore, nbaseDigitsAfter);
    }

    public BigDecimal toBigDecimal() {
        final BigInteger before = fromNbase(digits, 0, getDigitsBefore());
        final BigInteger after = fromNbase(digits, getDigitsBefore(), getDigitsAfter());
        return new BigDecimal(before).add(new BigDecimal(after, getDigitsAfter() * NBASE_DIGITS)).setScale(displayScale);
    }

    public void toBuffer(final ByteBuffer buffer) {
        buffer.putShort(numberDigits).putShort(weight).putShort(sign).putShort(displayScale);
        for(int i = 0; i < digits.length; ++i) {
            buffer.putShort(digits[i]);
        }
    }

    @Override
    public boolean equals(final Object rhs) {
        if(!(rhs instanceof PostgresNumeric)) {
            return false;
        }

        return equals((PostgresNumeric) rhs);
    }

    private boolean equals(final PostgresNumeric rhs) {
        return (numberDigits == rhs.numberDigits &&
                weight == rhs.weight &&
                sign == rhs.sign &&
                displayScale == rhs.displayScale &&
                Arrays.equals(digits, rhs.digits));
    }

    @Override
    public int hashCode() {
        int hash = 2371;
        hash += hash + numberDigits;
        hash += 2371 * hash + weight;
        hash += 2371 * hash + sign;
        hash += 2371 * hash + displayScale;
        hash += 2371 * hash + Arrays.hashCode(digits);
        return hash;
    }
}
