package ÒµÎñÍÆ¼ö;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;

public class ConnectionProxy implements InvocationHandler {
	private Object object;

	public Object bind(Object object) {
		this.object = object;
		return Proxy.newProxyInstance(object.getClass()
				.getClassLoader(), object.getClass().getInterfaces(),
				this);
	}
	
	public ConnectionProxy() {
		;
	}

	/*************************/
	public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		Object resultObject = null;
		Class classes[] = method.getParameterTypes();
		for (int i = 0; i < classes.length; i++) {
			if (classes[i].equals(Connection.class)) {
				Connection connection = null;
				try {
					connection = ((TopKRecommend)object).getConnection();
					args[i] = connection;
					resultObject = method.invoke(this.object, args);
					return resultObject;
				} catch (Exception e) {
					// TODO: handle exception
				} finally {
					((TopKRecommend)object).releaseConnection(connection);
				}
			}
		}

		return null;
	}

}