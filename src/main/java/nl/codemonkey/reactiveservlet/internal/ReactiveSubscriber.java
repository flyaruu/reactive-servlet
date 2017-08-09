package nl.codemonkey.reactiveservlet.internal;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReactiveSubscriber implements Subscriber<byte[]>, WriteListener {

	protected static final long INITIAL_REQUEST = 1;
	
	private final AsyncContext context;
	final ServletOutputStream out;
	final AtomicBoolean done = new AtomicBoolean(false);
	final AtomicBoolean completed = new AtomicBoolean(false);
	final AtomicBoolean outputReady = new AtomicBoolean(false);
	
	final AtomicLong totalWritten = new AtomicLong();
	final AtomicLong totalRequested = new AtomicLong();
	final AtomicLong bytesWritten = new AtomicLong();
//	final AtomicLong flushQueue = new AtomicLong();

	private final AtomicBoolean isInitial = new AtomicBoolean(true);
	final Queue<byte[]> outputQueue = new ConcurrentLinkedQueue<byte[]>();
	private volatile Subscription subscription;
	private final static Logger logger = LoggerFactory.getLogger(ReactiveSubscriber.class);

	public ReactiveSubscriber(AsyncContext asc) throws IOException {
		this.context = asc;
		out = context.getResponse().getOutputStream();
	}
	
	private long flushQueue() throws IOException {
		if(!outputReady.get()) {
			return 0;
		}
		long writeCount = 0;
		while (!outputQueue.isEmpty()) {
			boolean rd = out.isReady();
			if(!rd) {
				outputReady.set(false);
				logger.info("Breaking, output no longer ready Queue size: {}. Total written: {} bytes: {} requested: {}",outputQueue.size(), totalWritten.get(),bytesWritten.get(), totalRequested.get());
				return writeCount;
			}
			byte[] element = outputQueue.poll();
			bytesWritten.addAndGet(element.length);
			out.write(element);
			writeCount++;
		}
		if (done.get() && !completed.get() && outputQueue.isEmpty()) {
//			System.err.println("Done switch trapped and write complete, closing context.");
			completed.compareAndSet(false, true);
			context.complete();
		}
//		if(writeCount>0) {
//			long total = totalWritten.addAndGet(writeCount);
//			logger.debug("Written from flushQueue: "+total);
//			flushQueue.addAndGet(writeCount);
//			long l = elementsRequested.addAndGet(writeCount);
//		}
		return writeCount;
	}

	@Override
	public void onComplete() {
		try {
			logger.info("Input completed. Setting done: {} queue size: {} bytes written {}",done.get(),outputQueue.size(),bytesWritten.get());
			done.compareAndSet(false,true);
			logger.info("Write Done: "+done.get());
			if(!outputQueue.isEmpty()) {
				long written = flushQueue();
				return;
			}
			final boolean ready = out.isReady();
			if(ready && outputQueue.isEmpty()) {
				completed.compareAndSet(false, true);
				context.complete();
			} else {
				//	onCompleted done, but not ready, so deferring complete to onWritePossible
			}
		
		} catch (Exception e) {
			logger.error("Error: ", e);
		}
	}

	@Override
	public void onError(Throwable e) {
		logger.error("Servlet Error: ", e);
		context.complete();

	}

	@Override
	public synchronized void onNext(byte[] b) {
		try {
			outputQueue.offer(b);
			long written =  flushQueue();
			if(written > 0) {
				totalRequested.addAndGet(written);
				this.subscription.request(written);
				logger.info("Requesting: {} total requested: {}",written,totalRequested.get());
			}
			long total = totalWritten.addAndGet(written);
//			logger.debug("Written from onNext: {} bytes: {}",total,bytesWritten.get());

		} catch (Exception e) {
			logger.error("Error: ", e);
			onError(e);
			if(this.subscription!=null) {
				this.subscription.cancel();
			}
		}
	}

	@Override
	public void onSubscribe(Subscription s) {
		this.subscription = s;
		out.setWriteListener(this);
//		System.err.println("Request initial");
//		subscription.request(INITIAL_REQUEST);
	}
	
	@Override
	public synchronized void onWritePossible() throws IOException {
		logger.debug("Entering onWritePossible");
		if(isInitial.get()) {
			isInitial.set(false);
			subscription.request(INITIAL_REQUEST);
			totalRequested.addAndGet(INITIAL_REQUEST);

		}
		outputReady.set(true);
		long written =  flushQueue();
		if(written > 0) {
			this.subscription.request(written);
			totalRequested.addAndGet(written);
			logger.info("Requesting from possible: {} total requested: {}",written,totalRequested.get());

		}
//		subscription.request(10);
//		totalRequested.addAndGet(10);
//		logger.info("Written from onWritePossible: {} bytes: {} total requested: {}",this.totalWritten.get(), this.bytesWritten.get(),totalRequested.get());
//		logger.info("Exiting onWritePossible. Bytes written: {}",bytesWritten.get());
	}

}
