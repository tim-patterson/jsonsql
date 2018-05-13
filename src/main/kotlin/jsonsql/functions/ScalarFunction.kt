package jsonsql.functions

import java.time.Instant


abstract class OneArgScalarFunction: Function.ScalarFunction() {
    override fun execute(args: List<Any?>): Any? {
        return execute(args[0])
    }

    override fun validateParameterCount(count: Int) = count == 1

    abstract fun execute(arg1: Any?): Any?
}


abstract class TwoArgScalarFunction: Function.ScalarFunction() {
    override fun execute(args: List<Any?>): Any? {
        return execute(args[0], args[1])
    }

    override fun validateParameterCount(count: Int) = count == 2

    abstract fun execute(arg1: Any?, arg2: Any?): Any?
}

object CoalesceFunction: Function.ScalarFunction() {
    override fun validateParameterCount(count: Int) = true

    override fun execute(args: List<Any?>): Any? {
        return args.filterNotNull().firstOrNull()
    }

}

// gets components out of nested objects
object IndexFunction: TwoArgScalarFunction() {
    override fun execute(arg1: Any?, arg2: Any?): Any? {
        val map = MapInspector.inspect(arg1)
        map?.let {
            val key = StringInspector.inspect(arg2) ?: return null
            return map[key]
        }

        val array = ArrayInspector.inspect(arg1)
        array?.let {
            val key = NumberInspector.inspect(arg2) ?: return null
            return array.elementAtOrNull(key.toInt())
        }

        return null
    }
}

object AndFunction: TwoArgScalarFunction() {
    override fun execute(arg1: Any?, arg2: Any?): Any? {
        val val1 = BooleanInspector.inspect(arg1)
        val val2 = BooleanInspector.inspect(arg2)
        if (val1 == null || val2 == null) return null

        return val1 && val2
    }
}

object OrFunction: TwoArgScalarFunction() {
    override fun execute(arg1: Any?, arg2: Any?): Any? {
        val val1 = BooleanInspector.inspect(arg1)
        val val2 = BooleanInspector.inspect(arg2)
        if (val1 == null || val2 == null) return null

        return val1 || val2
    }
}

object IsNullFunction: OneArgScalarFunction() {
    override fun execute(arg1: Any?): Any? {
        return arg1 == null
    }
}

object IsNotNullFunction: OneArgScalarFunction() {
    override fun execute(arg1: Any?): Any? {
        return arg1 != null
    }
}

object NumberFunction: OneArgScalarFunction() {
    override fun execute(arg1: Any?): Any? {
        return NumberInspector.inspect(arg1)
    }
}

/**
 * Used for time windowing
 */
object TumbleFunction: TwoArgScalarFunction() {
    override fun execute(arg1: Any?, arg2: Any?): Any? {
        val val1 = TimestampInpector.inspect(arg1)
        val val2 = DurationInpector.inspect(arg2)
        if (val1 == null || val2 == null) return null

        // floor towards 1970
        return Instant.ofEpochMilli((val1.toEpochMilli() / val2.toMillis()) * val2.toMillis())
    }
}

/**
 * hopping(ts, 1min, 1hour)
 * for a ts of 12:59 it should appear in every window from
 * 12:00 - 12:59 (window start)
 */
object HoppingFunction: Function.ScalarFunction() {
    override fun validateParameterCount(count: Int) = count == 3
    override fun execute(args: List<Any?>): Any? {
        val val1 = TimestampInpector.inspect(args[0])
        val val2 = DurationInpector.inspect(args[1])
        val val3 = DurationInpector.inspect(args[2])
        if (val1 == null || val2 == null || val3 == null) return null
        // same as tumbling
        val baseMs = (val1.toEpochMilli() / val2.toMillis()) * val2.toMillis()

        val periodCount = val3.toMillis() / val2.toMillis()

        return (0 until periodCount).map {
            Instant.ofEpochMilli(baseMs - it *  val2.toMillis())
        }
    }
}