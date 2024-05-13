package com.redis.riot.resource;


import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Output stream that Keeps track of the number of bytes written by another output stream.
 *
 */
public class CountingOutputStream extends FilterOutputStream {

	private long count;

	public CountingOutputStream(OutputStream out) {
		super(out);
	}

	/**
	 * @return written byte count.
	 */
	public long getCount() {
		return count;
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		out.write(b, off, len);
		count += len;
	}

	@Override
	public void write(int b) throws IOException {
		out.write(b);
		count++;
	}

	@Override
	public void close() throws IOException {
		out.close();
	}

}
