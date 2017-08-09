package nl.codemonkey.reactiveservlet.example;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.AsyncContext;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.davidmoten.rx2.Bytes;

import hu.akarnokd.rxjava2.debug.validator.RxJavaProtocolValidator;
import io.reactivex.schedulers.Schedulers;
import nl.codemonkey.reactiveservlet.Servlets;

public class ReactiveServlet extends HttpServlet {

	private static final long serialVersionUID = 2921098868587223032L;
	protected static final int BUFFER_SIZE = 8192;
	
	private final static Logger logger = LoggerFactory.getLogger(ReactiveServlet.class);

	private AtomicLong counter = new AtomicLong();

	
	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);

//		RxJavaProtocolValidator.setOnViolationHandler(e -> e.printStackTrace());
//
//		RxJavaProtocolValidator.enable();
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		AsyncContext asc = req.startAsync();
		resp.setContentType("image/jpg");
		Bytes.from(getClass().getClassLoader().getResourceAsStream("image.jpg"),BUFFER_SIZE)
			.doOnError(e->e.printStackTrace())
			.subscribe(Servlets.createSubscriber(asc));

	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		AsyncContext asc = req.startAsync();
//		new Thread(){
//
//			@Override
//			public void run() {
//				try {
					Servlets.createFlowable(asc, BUFFER_SIZE)
					.observeOn(Schedulers.io(),false,10)
					.doOnNext(this::printCount)
					.subscribe(Servlets.createSubscriber(asc));
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
			
//		}.start();
	}
	
	private void printCount(byte[] b) {
		long n = counter.incrementAndGet();
		logger.info("Element count: {}",n);
	}
	
}
