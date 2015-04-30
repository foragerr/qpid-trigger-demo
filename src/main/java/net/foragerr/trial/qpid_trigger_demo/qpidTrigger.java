package net.foragerr.trial.qpid_trigger_demo;

import java.io.IOException;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.log4j.Logger;

public class qpidTrigger implements MessageListener {

	private final Logger log = Logger.getLogger(qpidTrigger.class);
	
	private String _lock = "lock";
	private boolean _finished = false;
	private boolean _failed = false;
	
	private Session session=null;
	private MessageProducer triggerQProducer = null;
	
	public static void main(String[] args) {
		qpidTrigger qt = new qpidTrigger();
		try {
			qt.runTrigger();
		} catch (Exception e) {
			qt.log.error(null, e);
		}
	}

	
	private void runTrigger() throws IOException, NamingException, JMSException, InterruptedException
	{
		Properties properties = new Properties();
		properties.load(this.getClass().getResourceAsStream("qpid.properties"));
		Context context = new InitialContext(properties);

		Destination mainQ = (Destination) context.lookup("mainqueue");
		Destination triggerQ = (Destination) context.lookup("triggerqueue");
		
		ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("qpidConnectionfactory");
		Connection connection = connectionFactory.createConnection();
		session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
		MessageConsumer mainQConsumer = session.createConsumer(mainQ);
		triggerQProducer = session.createProducer(triggerQ);
		
		// Set the listener and start the connection
		mainQConsumer.setMessageListener(this);
		connection.start();
		 
		// Wait for the shutdown message
		 synchronized (_lock)
		 {
		     while (!_finished && !_failed)
		     {
		         _lock.wait();
		     }
		 }
		 if (_failed)
		 {
		     log.error("invalid message(s)");
		 }

		 connection.close();
		 context.close();
	}
	
	public void onMessage(Message message)
	{
	    try
	    {	
	    	String text="";
	        if (message instanceof TextMessage)
	        {
	            text = ((TextMessage) message).getText();
	        }
	        if (text.equals("Shutdown!"))
	        {
	            log.info("Received shut down message, trigger monitor shutting down");
	            synchronized (_lock)
	            {
	                _finished = true;
	                _lock.notifyAll();
	            }
	        }
	        else
	        {
	            log.info("Received  a message on main queue, sending trigger message");
	            TextMessage triggerMessage = session.createTextMessage("Trigger-start-Application-X");
		    	this.triggerQProducer.send(triggerMessage);
	        }
	    }
	    catch (JMSException exp)
	    {
	    	log.error(": Caught an exception handling a received message", exp);
	        exp.printStackTrace();
	        synchronized (_lock)
	        {
	            _failed = true;
	            _lock.notifyAll();
	        }
	    }
	}

}
