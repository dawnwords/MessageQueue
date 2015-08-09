package com.alibaba.middleware.race.mom.store;

import java.util.List;

/**
 * Created by Dawnwords on 2015/8/7.
 */
public interface Storage {

    /**
     *
     *                  Storage Unit
     *
     *   0  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15
     *  +----------------------------------------------+
     *  |    ip    |    port   |        born time      |
     *  +----------------------------------------------+
     *  |   length |   offset  |   state   |           |
     *  +----------------------------------+           |
     *  |                   content                    |
     *  +----------------------------------------------+
     *
     */

    /**
     * Insert a storageUnit that has already been built
     *
     * @param header ip + port + born time + length + offset + <tt>MessageStatus.FAIL</tt>
     * @param body content
     * @return true if operation success, false otherwise
     */
    boolean insert(byte[] header, byte[]body );

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
    List<byte[]/* content */> failList();

}
