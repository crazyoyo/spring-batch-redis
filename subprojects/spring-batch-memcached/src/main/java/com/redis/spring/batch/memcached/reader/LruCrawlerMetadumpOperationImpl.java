package com.redis.spring.batch.memcached.reader;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.StringTokenizer;

import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationState;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.StatusCode;
import net.spy.memcached.protocol.BaseOperationImpl;

public class LruCrawlerMetadumpOperationImpl extends BaseOperationImpl {

	private static final OperationStatus END = new OperationStatus(true, "END", StatusCode.SUCCESS);
	private static final Charset CHARSET = StandardCharsets.UTF_8;

	private final ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
	private final LruCrawlerMetadumpOperation.Callback cb;
	private final byte[] msg;
	private byte[] errorMsg;

	public LruCrawlerMetadumpOperationImpl(String arg, LruCrawlerMetadumpOperation.Callback cb) {
		super();
		super.setCallback(cb);
		this.cb = cb;
		this.msg = ("lru_crawler metadump " + arg + "\r\n").getBytes();
	}

	private OperationErrorType classifyError(String line) {
		if (line.startsWith("ERROR")) {
			return OperationErrorType.GENERAL;
		}
		if (line.startsWith("CLIENT_ERROR")) {
			return OperationErrorType.CLIENT;
		}
		if (line.startsWith("SERVER_ERROR")) {
			return OperationErrorType.SERVER;
		}
		return null;
	}

	@Override
	public void readFromBuffer(ByteBuffer data) throws IOException {
		// Loop while there's data remaining to get it all drained.
		while (getState() != OperationState.COMPLETE && data.remaining() > 0) {
			int offset = -1;
			for (int i = 0; data.remaining() > 0; i++) {
				byte b = data.get();
				if (b == '\n') {
					offset = i;
					break;
				} else if (b != '\r') {
					byteBuffer.write(b);
				}
			}
			if (offset >= 0) {
				String line = byteBuffer.toString(CHARSET);
				byteBuffer.reset();
				OperationErrorType eType = classifyError(line);
				if (eType != null) {
					errorMsg = line.getBytes();
					handleError(eType, line);
				} else {
					handleLine(line);
				}
			}
		}
	}

	public void handleLine(String line) {
		if (line.equals("END")) {
			cb.receivedStatus(END);
			transitionState(OperationState.COMPLETE);
			return;
		}

		StringTokenizer tokenizer = new StringTokenizer(line, " ");
		LruMetadumpEntry entry = new LruMetadumpEntry();
		while (tokenizer.hasMoreTokens()) {
			String part = tokenizer.nextToken();
			int pos = part.indexOf('=');
			if (pos < 2) {
				continue;
			}
			String name = part.substring(0, pos);
			String value = part.substring(pos + 1);
			switch (name) {
			case "key":
				entry.setKey(decodeKey(value));
				break;
			case "exp":
				entry.setExp(decodeInt(value));
				break;
			case "la":
				entry.setLa(decodeInt(value));
				break;
			case "cas":
				entry.setCas(decodeInt(value));
				break;
			case "fetch":
				entry.setFetch("yes".equalsIgnoreCase(value));
				break;
			case "cls":
				entry.setCls(decodeInt(value));
				break;
			case "size":
				entry.setSize(decodeInt(value));
				break;
			default:
				break;
			}
		}

		cb.gotMetadump(entry);
	}

	private int decodeInt(String value) {
		return Integer.parseInt(value);
	}

	private String decodeKey(String value) {
		return URLDecoder.decode(value, CHARSET);
	}

	@Override
	public byte[] getErrorMsg() {
		return errorMsg;
	}

	@Override
	public void initialize() {
		setBuffer(ByteBuffer.wrap(msg));
	}

	@Override
	protected void wasCancelled() {
		cb.receivedStatus(CANCELLED);
	}

	@Override
	public String toString() {
		return "Cmd: " + Arrays.toString(msg);
	}
}