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
	
	final AtomicLong flushQueue = new AtomicLong();

	private final AtomicBoolean isInitial = new AtomicBoolean(true);
	final Queue<byte[]> outputQueue = new ConcurrentLinkedQueue<byte[]>();
	private volatile Subscription subscription;
	private final static Logger logger = LoggerFactory.getLogger(ReactiveSubscriber.class);

	public ReactiveSubscriber(AsyncContext asc) throws IOException {
		this.context = asc;
		out = context.getResponse().getOutputStream();
	}
	
	private synchronized void flushQueue() throws IOException {
		if(!outputReady.get()) {
			return;
		}
		int writeCount = 0;
		while (!outputQueue.isEmpty()) {
			boolean rd = out.isReady();
			if(!rd) {
				outputReady.set(false);
				System.err.println("Breaking, output no longer readyå");
				return;
			}
			byte[] element = outputQueue.poll();
//			byteWritten.addAndGet(element.length);
			out.write(element);
//			elementsWritten.incrementAndGet();
			writeCount++;
		}
		if (done.get() && !completed.get() && outputQueue.isEmpty()) {
			System.err.println("Done switch trapped and write complete, closing context.");
			completed.compareAndSet(false, true);
			context.complete();
		}
		if(writeCount>0) {
			flushQueue.addAndGet(writeCount);
//			long l = elementsRequested.addAndGet(writeCount);
		}
	}

	@Override
	public void onComplete() {
		try {
//			System.err.println("Input completed. Setting done: "+done.get()+" queue size: "+outputQueue.size()+" elements received: "+elementsReceived.get()+" bytes received: "+bytesReceived.get());
			done.compareAndSet(false,true);
//			System.err.println("Write Done: "+done.get());
			if(!outputQueue.isEmpty()) {
				flushQueue();
				return;
			}
			final boolean ready = out.isReady();
			System.err.println("Ready? "+ready);
			if(ready && outputQueue.isEmpty()) {
//				System.err.println(">onCompleted done, queue empty and still ready, so closing context:");
				completed.compareAndSet(false, true);
				context.complete();
			} else {
//				System.err.println("onCompleted done, but not ready, so deferring complete to onWritePossible");
			}
		
		} catch (Exception e) {
			logger.error("Error: ", e);
		}
	}

	@Override
	public void onError(Throwable e) {
		logger.error("Error: ", e);
		context.complete();

	}

	@Override
	public void onNext(byte[] b) {
		try {
			outputQueue.offer(b);
			flushQueue();
			long written = flushQueue.getAndSet(0);
			if(written > 0) {
				this.subscription.request(written);
			}
		} catch (Exception e) {
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
		subscription.request(INITIAL_REQUEST);
	}
	
	@Override
	public void onWritePossible() throws IOException {
		if(isInitial.get()) {
			isInitial.set(false);
		}
		outputReady.set(true);
		flushQueue();
		long written = flushQueue.getAndSet(0);
		if(written > 0) {
			this.subscription.request(written);
		}
		subscription.request(1);
	}

}
