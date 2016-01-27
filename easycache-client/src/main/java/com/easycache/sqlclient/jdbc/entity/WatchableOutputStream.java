package com.easycache.sqlclient.jdbc.entity;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * A java.io.OutputStream used to write ASCII data into Blobs and Clobs
 * 
 * @author Mark Matthews
 */
class WatchableOutputStream extends ByteArrayOutputStream {
	// ~ Instance fields
	// --------------------------------------------------------

	private OutputStreamWatcher watcher;

	// ~ Methods
	// ----------------------------------------------------------------

	/**
	 * @see java.io.OutputStream#close()
	 */
	public void close() throws IOException {
		super.close();

		if (this.watcher != null) {
			this.watcher.streamClosed(this);
		}
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @param watcher
	 *            DOCUMENT ME!
	 */
	public void setWatcher(OutputStreamWatcher watcher) {
		this.watcher = watcher;
	}
}
