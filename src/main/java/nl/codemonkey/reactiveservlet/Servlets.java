package nl.codemonkey.reactiveservlet;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.AsyncContext;
import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.reactivex.Flowable;
import nl.codemonkey.reactiveservlet.internal.ReactiveSubscriber;

public class Servlets {

	private final static Logger logger = LoggerFactory.getLogger(Servlets.class);

	private enum MODE {
		NORMAL,
		FINISHED,
		ERROR
	}
	
	public static Subscriber<byte[]> createSubscriber(AsyncContext asc) throws IOException {
		return new ReactiveSubscriber(asc);
	}
	
	public static Flowable<byte[]> createFlowable(AsyncContext asc, int bufferSize) throws IOException {
		final  AtomicLong readyReads = new AtomicLong(0);
		final  AtomicLong dataEmitted = new AtomicLong(0);
		final  AtomicLong dataRequested = new AtomicLong(0);
		final  AtomicLong bytesEmitted = new AtomicLong(0);
		final  AtomicLong bytesRead = new AtomicLong(0);
		ServletInputStream in = asc.getRequest().getInputStream();
		final AtomicBoolean isReady = new AtomicBoolean(false);
		final AtomicBoolean isInitialized = new AtomicBoolean(false);
		return new Flowable<byte[]>() {

			volatile MODE mode = MODE.NORMAL;
			
			private Subscription subscription;
			@Override
			protected void subscribeActual(Subscriber<? super byte[]> s) {
				this.subscription = new Subscription() {
					
					@Override
					public void request(long n) {
						readyReads.addAndGet(n);
						logger.info("More requests: {} total requested: {} emitted: {} diff: {}",readyReads.get(),dataRequested.get(),dataEmitted.get(),(dataRequested.get()-dataEmitted.get()));
						// only start polling once an initial onDataAvailable has been called
						dataRequested.addAndGet(n);
						if(isInitialized.get()) {
							mode = pollReadyData(in, s,mode);							
						}
					}
					
					@Override
					public void cancel() {
						try {
							in.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				};
				in.setReadListener(new ReadListener(){

					@Override
					public void onAllDataRead() throws IOException {
						logger.debug(">>>>>>>>>All data read");
						s.onComplete();
					}

					@Override
					public void onDataAvailable() throws IOException {
						logger.trace("Data ready!");
						isInitialized.set(true);
						isReady.set(true);
						mode = pollReadyData(in, s,mode);
//						if(mode==MODE.FINISHED) {
//							s.onComplete();
//						}
					}

					@Override
					public void onError(Throwable e) {
						e.printStackTrace();
					}
				});
				s.onSubscribe(this.subscription);

			}

			
			private synchronized MODE pollReadyData(ServletInputStream in, Subscriber<? super byte[]> s, MODE mode) {
			    byte[] buf = new byte[bufferSize];
				try {
					switch (mode) {
					case FINISHED:
						return MODE.FINISHED;
					case ERROR:
						throw new RuntimeException("Cant re-poll a failed flowable");
					case NORMAL:
						while(in.isReady()) {
						    if(in.isFinished()) {
						    	logger.info("Finished!");
						    	break;
						    }
						    long reads = readyReads.get();
//						    logger.info("-> Left: {} readyReads: {}",(dataRequested.get()-dataEmitted.get()),readyReads.get());
						    if(reads<=0) {
						    	logger.info("backpressuring. Data emitted: {} requested: {} ready reads: {}",dataEmitted.get(), dataRequested.get(),readyReads.get());
						    	return MODE.NORMAL;
						    }						    
							int len = in.read(buf);
						    if(len < 0) {
						    	return MODE.FINISHED;
						    }
						    long totalRead = bytesRead.addAndGet(len);
						    byte[] copy = Arrays.copyOf(buf, len);
						    readyReads.decrementAndGet();
						    s.onNext(copy);
						    long ee = dataEmitted.incrementAndGet();
						    long bts = bytesEmitted.addAndGet(copy.length);
						    logger.trace("Emitted: {} byte: {}",ee,bts);
						}
						isReady.set(false);
						logger.debug("Data no (longer) ready.");
						if(in.isFinished()) {
							logger.debug("Request finished: {} bytes",bytesEmitted.get());
//							in.close();
							return MODE.FINISHED;
						}
						return MODE.NORMAL;
					}
				} catch (IOException e) {
					try {
						in.close();
					} catch (IOException e1) {
						logger.error("Error: ", e1);
					}
					asc.complete();
					s.onError(e);
					return MODE.ERROR;
				}
				return mode;
			}
			
		};
	}

	
}
