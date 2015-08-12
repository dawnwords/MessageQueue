package com.alibaba.middleware.race.mom.store;

import com.alibaba.middleware.race.mom.bean.MessageId;

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
     * @param unit     ip + port + born time + length
     *                 + offset + <tt>MessageStatus.FAIL</tt>
     *                 + content
     * @param callback return true if operation success, false otherwise
     */
    void insert(StorageUnit unit, StorageCallback<Boolean> callback);

    /**
     * Mark the state for the message with given id as <tt>MessageStatus.SUCCESS</tt> asynchronously
     *
     * @param id id of the message to be marked
     */
    void markSuccess(MessageId id);

    /**
     * Mark the state for the message with given id as <tt>MessageStatus.FAIL</tt> asynchronously
     *
     * @param id id of the message to be marked
     */
    void markFail(MessageId id);

    /**
     * Get at most <tt>Parameter.PRODUCER_TIME_OUT_SECOND</tt> messages with the
     * state of <tt>MessageStatus.FAIL</tt> and mark their status as
     * <tt>MessageStatus.RESEND</tt>
     *
     * @param callback return those selected messages
     */
    void failList(StorageCallback<List<StorageUnit/* content */>> callback);


}
