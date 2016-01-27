package com.easycache.sqlclient.jdbc.entity;

import java.io.CharArrayWriter;

/**
 * A java.io.Writer used to write unicode data into Blobs and Clobs
 * 
 * @author Mark Matthews
 */
class WatchableWriter extends CharArrayWriter {
	// ~ Instance fields
	// --------------------------------------------------------

	private WriterWatcher watcher;

	// ~ Methods
	// ----------------------------------------------------------------

	/**
	 * @see java.io.Writer#close()
	 */
	public void close() {
		super.close();

		// Send data to watcher
		if (this.watcher != null) {
			this.watcher.writerClosed(this);
		}
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @param watcher
	 *            DOCUMENT ME!
	 */
	public void setWatcher(WriterWatcher watcher) {
		this.watcher = watcher;
	}
}
