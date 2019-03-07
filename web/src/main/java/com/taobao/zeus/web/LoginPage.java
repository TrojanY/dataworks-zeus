package com.taobao.zeus.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;  
import javax.servlet.http.HttpServletResponse;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;
import com.taobao.zeus.store.UserManager;
import com.taobao.zeus.store.mysql.persistence.ZeusUser;


public class LoginPage  extends HttpServlet  {

	private static final long serialVersionUID = 1L;
	private UserManager userManager;
	
	
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		String url  = "login2.html";
		request.getRequestDispatcher(url).forward(request,response);
		return;  
		
	}
	@Override
	public void init(ServletConfig servletConfig ) throws ServletException {
		ApplicationContext applicationContext=WebApplicationContextUtils.getWebApplicationContext(servletConfig.getServletContext());
		userManager=(UserManager) applicationContext.getBean("userManager");
	}
	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		
		//String url  = "/login.html";
		//request.getRequestDispatcher(url).forward(request,response);
		String username = request.getParameter("username");
        String password = request.getParameter("password");
        
        ZeusUser u = userManager.findByUidFilter(username);
        PrintWriter out = response.getWriter();
        response.setCharacterEncoding("utf-8");
		if(null == u){
			out.print("null");
		}else{
			String ps = u.getPassword();

			if(null !=ps){
				if(!MD5(password).toUpperCase().equals(ps.toUpperCase())){
					out.print("error");
					return;
				}
			}

			String uid = u.getUid();

			ZeusUser.USER.setUid(uid);
			ZeusUser.USER.setEmail(u.getEmail());
			ZeusUser.USER.setName(u.getName());
			ZeusUser.USER.setPhone(u.getPhone());
			
			Cookie cookie = new Cookie("LOGIN_USERNAME", uid);
			String host = request.getServerName();  
			cookie.setPath("/");  
			//cookie.setDomain(host);  
			response.addCookie(cookie);
			request.getSession().setAttribute("user", uid);
			LoginUser.user.set(ZeusUser.USER);
			out.print(uid);
		}
	}

	private static String MD5(String sourceStr) {
        String result = "";
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(sourceStr.getBytes());
            byte b[] = md.digest();
            int i;
            StringBuffer buf = new StringBuffer("");
            for (int offset = 0; offset < b.length; offset++) {
                i = b[offset];
                if (i < 0)
                    i += 256;
                if (i < 16)
                    buf.append("0");
                buf.append(Integer.toHexString(i));
            }
            result = buf.toString();
        } catch (NoSuchAlgorithmException e) {
            System.out.println(e);
        }
        return result;
    }

}
