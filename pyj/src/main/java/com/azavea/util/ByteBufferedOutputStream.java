package com.azavea.util;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * https://gist.github.com/manzke/985007
 *
 * The MIT License (MIT)
 *  Copyright (c) 2015 Daniel Manzke
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */
public class ByteBufferedOutputStream extends OutputStream {
    private ByteBuffer buffer;
    private boolean onHeap;
    private float increasing = DEFAULT_INCREASING_FACTOR;

    public static final float DEFAULT_INCREASING_FACTOR = 1.5f;

    public ByteBufferedOutputStream(int size) {
        this(size, DEFAULT_INCREASING_FACTOR, false);
    }

    public ByteBufferedOutputStream(int size, boolean onHeap) {
        this(size, DEFAULT_INCREASING_FACTOR, onHeap);
    }

    public ByteBufferedOutputStream(int size, float increasingBy) {
        this(size, increasingBy, false);
    }

    public ByteBufferedOutputStream(int size, float increasingBy, boolean onHeap) {
        if(increasingBy <= 1){
            throw new IllegalArgumentException("Increasing Factor must be greater than 1.0");
        }
        if(onHeap){
            buffer = ByteBuffer.allocate(size);
        }else{
            buffer = ByteBuffer.allocateDirect(size);
        }
        this.onHeap = onHeap;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int position = buffer.position();
        int limit = buffer.limit();

        long newTotal = position + len;
        if(newTotal > limit){
            int capacity = (int) (buffer.capacity()*increasing);
            while(capacity <= newTotal){
                capacity = (int) (capacity*increasing);
            }

            increase(capacity);
        }

        buffer.put(b, 0, len);
    }

    @Override
    public void write(int b) throws IOException {
        if(!buffer.hasRemaining()){
            increase((int) (buffer.capacity()*increasing));
        }
        buffer.put((byte)b);
    }

    protected void increase(int newCapacity){
        buffer.limit(buffer.position());
        buffer.rewind();

        ByteBuffer newBuffer;
        if(onHeap){
            newBuffer = ByteBuffer.allocate(newCapacity);
        }else{
            newBuffer = ByteBuffer.allocateDirect(newCapacity);
        }

        newBuffer.put(buffer);
        buffer.clear();
        buffer = newBuffer;
    }

    public long size(){
        return buffer.position();
    }

    public long capacity(){
        return buffer.capacity();
    }

    public ByteBuffer buffer(){
        return buffer;
    }
}