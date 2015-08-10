package com.alibaba.middleware.race.mom.store;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by Dawnwords on 2015/8/7.
 */
public interface Storage {

    void start();

    void stop();

    /**
     * Insert a storageUnit that has already been built
     *
     * @param unit ip + port + born time + length
     *             + offset + <tt>MessageStatus.FAIL</tt>
     *             + content
     * @return true if operation success, false otherwise
     */
    boolean insert(StorageUnit unit);

    /**
     * Mark the state for the message with given id as <tt>MessageStatus.SUCCESS</tt>
     *
     * @param id id of the message to be marked
     * @return true if the operation succeed, false otherwise
     */
    boolean markSuccess(byte[] id);

    /**
     * Mark the state for the message with given id as <tt>MessageStatus.FAIL</tt>
     *
     * @param id id of the message to be marked
     * @return true if the operation succeed, false otherwise
     */
    boolean markFail(byte[] id);

    /**
     * Get at most <tt>Parameter.PRODUCER_TIME_OUT_SECOND</tt> messages with the
     * state of <tt>MessageStatus.FAIL</tt> and mark their status as
     * <tt>MessageStatus.RESEND</tt>
     *
     * @return those selected messages
     */
    List<StorageUnit/* content */> failList();

}
