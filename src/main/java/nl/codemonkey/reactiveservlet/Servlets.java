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

//	protected static final int BUFFER_SIZE = 8192;

	private enum MODE {
		INITIAL,
		BACKPRESSURED,
		FLOWING,
		FINISHED,
		ERROR
	}
	
	public static Subscriber<byte[]> createSubscriber(AsyncContext asc) throws IOException {
		return new ReactiveSubscriber(asc);
	}
	
	public static Flowable<byte[]> createFlowable(AsyncContext asc, int bufferSize) throws IOException {
		final  AtomicLong readyReads = new AtomicLong(0);
		ServletInputStream in = asc.getRequest().getInputStream();
		final AtomicBoolean isReady = new AtomicBoolean(false);
		return new Flowable<byte[]>() {

			volatile MODE mode = MODE.INITIAL;
			
			private Subscription subscription;
			@Override
			protected void subscribeActual(Subscriber<? super byte[]> s) {
				this.subscription = new Subscription() {
					
					@Override
					public void request(long n) {
						readyReads.addAndGet(n);
						mode = pollReadyData(in, s,mode);
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
						System.err.println("All data read");
						s.onComplete();
					}

					@Override
					public void onDataAvailable() throws IOException {
						isReady.set(true);
						mode = pollReadyData(in, s,mode);
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
					case INITIAL:
					case BACKPRESSURED:
					case FLOWING:
						while(in.isReady()) {
						    if(in.isFinished()) {
						    	System.err.println("Finished!");
						    	break;
						    }
						    long reads = readyReads.get();
						    if(reads<=0) {
						    	return MODE.BACKPRESSURED;
						    }						    
							int len = in.read(buf);
						    if(len < 0) {
						    	return MODE.FINISHED;
						    }
						    byte[] copy = Arrays.copyOf(buf, len);
						    readyReads.decrementAndGet();
						    s.onNext(copy);
						}
						isReady.set(false);
						if(in.isFinished()) {
							return MODE.FINISHED;
						}
						return MODE.FLOWING;
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
