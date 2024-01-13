import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReferenceArray

/**
 * @author Belousov Timofey
 */
class FlatCombiningQueue<E> : Queue<E> {
    private val queue = ArrayDeque<E>() // sequential queue
    private val combinerLock = AtomicBoolean(false) // unlocked initially
    private val tasksForCombiner = AtomicReferenceArray<Any?>(TASKS_FOR_COMBINER_SIZE)

    private fun tryLock(): Boolean {
        return combinerLock.compareAndSet(false, true)
    }

    private fun unlock() {
        combinerLock.set(false)
    }

    // Must already hold the lock
    @Suppress("UNCHECKED_CAST")
    private fun combine() {
        for (i in 0..<tasksForCombiner.length()) {
            val curTask = tasksForCombiner.get(i)

            if (curTask == null || curTask is Result<*>) {
                continue
            }

            if (curTask == Dequeue) {
                tasksForCombiner.set(i, Result(queue.removeFirstOrNull()))
                continue
            }

            queue.addLast(curTask as E)
            tasksForCombiner.set(i, Result(Any()))
        }
    }

    override fun enqueue(element: E) {
        var announcedId = -1

        while (!tryLock()) {
            if (announcedId < 0) {
                val i = randomCellIndex()
                if (tasksForCombiner.compareAndSet(i, null, element)) {
                    announcedId = i
                }
            } else {
                val curVal = tasksForCombiner.get(announcedId)
                if (curVal is Result<*>) {
                    tasksForCombiner.set(announcedId, null)
                    return
                }
            }
        }

        if (announcedId < 0) {
            queue.addLast(element)
        }

        combine()
        unlock()
    }

    @Suppress("UNCHECKED_CAST")
    override fun dequeue(): E? {
        var announcedId = -1

        while (!tryLock()) {
            if (announcedId < 0) {
                val i = randomCellIndex()
                if (tasksForCombiner.compareAndSet(i, null, Dequeue)) {
                    announcedId = i
                }
            } else {
                val curVal = tasksForCombiner.get(announcedId)
                if (curVal is Result<*>) {
                    tasksForCombiner.set(announcedId, null)
                    return (curVal as Result<E?>).value
                }
            }
        }

        combine()

        val result: E?
        if (announcedId < 0) {
            result = queue.removeFirstOrNull()
        } else {
            val announcedState = tasksForCombiner.getAndSet(announcedId, null)
            if (announcedState is Result<*>) {
                result = (announcedState as Result<E?>).value
            } else {
                throw IllegalStateException("WTF?")
            }
        }

        unlock()

        return result
    }

    private fun randomCellIndex(): Int =
        ThreadLocalRandom.current().nextInt(tasksForCombiner.length())
}

private const val TASKS_FOR_COMBINER_SIZE = 3 // Do not change this constant!

private object Dequeue

private class Result<V>(
    val value: V
)
