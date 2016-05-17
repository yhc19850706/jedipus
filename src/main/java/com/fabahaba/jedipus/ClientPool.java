package com.fabahaba.jedipus;

import java.util.NoSuchElementException;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;

public interface ClientPool<C> {
  /**
   * Obtains an instance from this pool.
   * <p>
   * Instances returned from this method will have been either newly created with
   * {@link PooledObjectFactory#makeObject} or will be a previously idle object and have been
   * activated with {@link PooledObjectFactory#activateObject} and then validated with
   * {@link PooledObjectFactory#validateObject}.
   * <p>
   * By contract, clients <strong>must</strong> return the borrowed instance using
   * {@link #returnObject}, {@link #invalidateObject}, or a related method as defined in an
   * implementation or sub-interface.
   * <p>
   * The behaviour of this method when the pool has been exhausted is not strictly specified
   * (although it may be specified by implementations).
   *
   * @return an instance from this pool.
   *
   * @throws IllegalStateException after {@link #close close} has been called on this pool.
   * @throws Exception when {@link PooledObjectFactory#makeObject} throws an exception.
   * @throws NoSuchElementException when the pool is exhausted and cannot or will not return another
   *         instance.
   */
  C borrowObject() throws Exception, NoSuchElementException, IllegalStateException;

  /**
   * Return an instance to the pool. By contract, <code>obj</code> <strong>must</strong> have been
   * obtained using {@link #borrowObject()} or a related method as defined in an implementation or
   * sub-interface.
   *
   * @param obj a {@link #borrowObject borrowed} instance to be returned.
   *
   * @throws IllegalStateException if an attempt is made to return an object to the pool that is in
   *         any state other than allocated (i.e. borrowed). Attempting to return an object more
   *         than once or attempting to return an object that was never borrowed from the pool will
   *         trigger this exception.
   *
   * @throws Exception if an instance cannot be returned to the pool
   */
  void returnObject(C obj) throws Exception;

  /**
   * Invalidates an object from the pool.
   * <p>
   * By contract, <code>obj</code> <strong>must</strong> have been obtained using
   * {@link #borrowObject} or a related method as defined in an implementation or sub-interface.
   * <p>
   * This method should be used when an object that has been borrowed is determined (due to an
   * exception or other problem) to be invalid.
   *
   * @param obj a {@link #borrowObject borrowed} instance to be disposed.
   *
   * @throws Exception if the instance cannot be invalidated
   */
  void invalidateObject(C obj) throws Exception;

  /**
   * Create an object using the {@link PooledObjectFactory factory} or other implementation
   * dependent mechanism, passivate it, and then place it in the idle object pool.
   * <code>addObject</code> is useful for "pre-loading" a pool with idle objects. (Optional
   * operation).
   *
   * @throws Exception when {@link PooledObjectFactory#makeObject} fails.
   * @throws IllegalStateException after {@link #close} has been called on this pool.
   * @throws UnsupportedOperationException when this pool cannot add new idle objects.
   */
  void addObject() throws Exception, IllegalStateException, UnsupportedOperationException;

  /**
   * Return the number of instances currently idle in this pool. This may be considered an
   * approximation of the number of objects that can be {@link #borrowObject borrowed} without
   * creating any new instances. Returns a negative value if this information is not available.
   * 
   * @return the number of instances currently idle in this pool.
   */
  int getNumIdle();

  /**
   * Return the number of instances currently borrowed from this pool. Returns a negative value if
   * this information is not available.
   * 
   * @return the number of instances currently borrowed from this pool.
   */
  int getNumActive();

  /**
   * Clears any objects sitting idle in the pool, releasing any associated resources (optional
   * operation). Idle objects cleared must be
   * {@link PooledObjectFactory#destroyObject(PooledObject)}.
   *
   * @throws UnsupportedOperationException if this implementation does not support the operation
   *
   * @throws Exception if the pool cannot be cleared
   */
  void clear() throws Exception, UnsupportedOperationException;

  /**
   * Close this pool, and free any resources associated with it.
   * <p>
   * Calling {@link #addObject} or {@link #borrowObject} after invoking this method on a pool will
   * cause them to throw an {@link IllegalStateException}.
   * <p>
   * Implementations should silently fail if not all resources can be freed.
   */
  void close();
}