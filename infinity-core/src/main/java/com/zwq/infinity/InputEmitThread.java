package com.zwq.infinity;

import com.zwq.infinity.baseplugin.BaseInput;

/**
 * created by kris on 3/19/19.
 * @apiNote 继承了Thread, 调用其start方法时, 会执行run方法
 */
public class InputEmitThread extends Thread {
    private final BaseInput input;

    InputEmitThread(BaseInput input) {
        this.input = input;
    }

    @Override
    public void run() {
        this.input.emit();
    }
}