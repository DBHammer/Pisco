package lock;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.Test;

public class LockTest {
  @Test
  public void someTest() {
    ReadWriteLock lock = new ReentrantReadWriteLock();
    //        lock.readLock().lock();
    //        lock.readLock().lock();
    //        lock.readLock().lock();
    //        lock.readLock().unlock();
    //        lock.readLock().unlock();
    //        lock.readLock().unlock();
    //        lock.readLock().lock();
    lock.writeLock().lock();
    lock.writeLock().lock();
    lock.writeLock().lock();
    System.out.println();
  }
}
