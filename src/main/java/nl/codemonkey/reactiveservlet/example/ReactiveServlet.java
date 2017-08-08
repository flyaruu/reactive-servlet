package nl.codemonkey.reactiveservlet.example;
import java.io.File;
import java.io.IOException;

import javax.servlet.AsyncContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.github.davidmoten.rx2.Bytes;

import io.reactivex.schedulers.Schedulers;
import nl.codemonkey.reactiveservlet.Servlets;

public class ReactiveServlet extends HttpServlet {

	private static final long serialVersionUID = 2921098868587223032L;
	protected static final int BUFFER_SIZE = 8192;


	
	public ReactiveServlet() throws IOException {
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		AsyncContext asc = req.startAsync();
		resp.setContentType("image/jpg");
		Bytes.from(new File("IMG_1435.jpg"),BUFFER_SIZE)
			.subscribe(Servlets.createSubscriber(asc));

	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		AsyncContext asc = req.startAsync();
		Servlets.createFlowable(asc, BUFFER_SIZE)
				.observeOn(Schedulers.io(),false,10)
				.subscribe(Servlets.createSubscriber(asc));
	}
	
}
