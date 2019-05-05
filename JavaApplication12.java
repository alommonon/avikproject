package JavaApplication12;

import java.sql.SQLException;
import java.text.DecimalFormat;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * A server program which accepts requests from clients to
 * capitalize strings.  When clients connect, a new thread is
 * started to handle an interactive dialog in which the client
 * sends in a string and the server thread sends back the
 * capitalized version of the string.
 *
 * The program is runs in an infinite loop, so shutdown in platform
 * dependent.  If you ran it from a console window with the "java"
 * interpreter, Ctrl+C generally will shut it down.
 */
public class JavaApplication12 {
    
    Connection conn = null;


    /**
     * Application method to run the server runs in an infinite loop
     * listening on port 9898.  When a connection is requested, it
     * spawns a new thread to do the servicing and immediately returns
     * to listening.  The server keeps a unique client number for each
     * client that connects just to show interesting logging
     * messages.  It is certainly not necessary to do this.
     */
    public static void main(String[] args) throws Exception {
        System.out.println("The capitalization server is running.");
        
        int clientNumber = 0;
        ServerSocket listener = new ServerSocket(3000);
        
        Class.forName("oracle.jdbc.OracleDriver");
        
         
        
            
            
        try {
            while (true) {
                new Capitalizer(listener.accept(), clientNumber++).start();
            }
        } finally {
            listener.close();
           
            
        }
    }

    /**
     * A private thread to handle capitalization requests on a particular
     * socket.  The client terminates the dialogue by sending a single line
     * containing only a period.
     */
    private static class Capitalizer extends Thread {
    
        
          private Socket socket;
        private int clientNumber;
        private BufferedReader in;
        private BufferedWriter out;
        private PrintWriter pw;
        public DataOutputStream dout;
        public DataInputStream din;
        public Connection conn = null;
        //static DataOutputStream dout;
        //private DataOutputStream dout;
       
        //static DataOutputStream dout;
        //private DataOutputStream dout;
       

        public Capitalizer(Socket socket, int clientNumber) throws SQLException {
            this.socket = socket;
            this.clientNumber = clientNumber;
             conn = DriverManager.getConnection("jdbc:oracle:thin:@203.188.246.141:1521:orcl","vehicle","mononsoft");
            
                // TODO code application logic here
            System.out.println("Successfully Connected"); 
            
            log("New connection with client# " + clientNumber + " at " + socket);
        }

        /**
         * Services this thread's client by first sending the
         * client a welcome message then repeatedly reading strings
         * and sending back the capitalized version of the string.
         */
        public void run() {
            try {

                // Decorate the streams so we can send characters
                // and not just bytes.  Ensure output is flushed
                // after every newline.
               // in = new BufferedReader(
                        //new InputStreamReader(socket.getInputStream()));
                ////out = new BufferedWriter(
                        //new OutputStreamWriter(socket.getOutputStream()));
               
                //dout = new DataOutputStream(socket.getOutputStream());
                
                //pw = new PrintWriter(socket.getOutputStream());
                
                din = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
                dout = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
               
                // Send a welcome message to the client.
                //out.println("Hello, you are client #" + clientNumber + ".");
                //out.println("Enter a line with only a period to quit\n");

                // Get messages from the client, line by line; return them
                // capitalized
                //out.write("**,imei:864180035993786,100");
                
                
                //String clientData= in.readUTF();
                
                
                
                while (true) {
                   
                    byte[] byteData = null;
                    String location=null;
                try {
                    byteData = receiveData(din);
                } catch (Exception ex) {
                    Logger.getLogger(JavaApplication12.class.getName()).log(Level.SEVERE, null, ex);
                }
                String clientRequestMessage = new String(byteData).trim();
                String clientData = doProcess(clientRequestMessage);
                if(clientData==null){
                    String [] check = clientRequestMessage.split(":");
                    
                    log("final Message : "+clientRequestMessage+" String Length : "+clientRequestMessage.length());
			String [] arr = clientRequestMessage.split(",");
			//int i=0;
                        if( !"imei".equals(check[0])||"L".equals(arr[4])){  //||"L".equals(arr[4])
                        
                            System.out.println("Unauthorized value supplied by device");
                            break;
                    }
			//for(String a : arr)
			    //System.out.println(a);
                            String [] arr1 = arr[0].split(":");
                            
                        
                            
                            //To display Latitute and Longtitude
			    System.out.println("Latitude  : "+degree_to_decimal(arr[7],arr[8]));
			    System.out.println("Longitude : "+degree_to_decimal(arr[9],arr[10]));
                        try {
                             location = getAddressByGpsCoordinates(String.valueOf(degree_to_decimal(arr[9],arr[10])),String.valueOf(degree_to_decimal(arr[7],arr[8])));
                            if ("".equals(location) || location==null){
                                location="No location";
                            }
                             //end to display Latitude and Longditude
                        } catch (MalformedURLException ex) {
                            Logger.getLogger(JavaApplication12.class.getName()).log(Level.SEVERE, null, ex);
                        } catch (org.json.simple.parser.ParseException ex) {
                            Logger.getLogger(JavaApplication12.class.getName()).log(Level.SEVERE, null, ex);
                        }
                            
                            // concat date
                            char[] datearr = arr[2].toCharArray();
                            String date= datearr[5] + "/" + datearr[2] + datearr[3] + "/20"+datearr[0]+datearr[1]+" "+datearr[6]+datearr[7]+":"+ datearr[8]+datearr[9]+":"+datearr[10]+datearr[11];
                            String datew= datearr[4] + date;
                            System.out.println("datew : "+datew);
                            //concat date end
//                            for(int i=0;i<datearr.length;i++){
//                            System.out.println(arr[2]+"   "+datearr[i]+"  "+datew);
//                            }
                            // find maximum value to insert into Column "ID"
                            DateFormat formatter;
                            java.util.Date dob;
                            formatter = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
                            dob = formatter.parse(datew);
                            System.out.println("String Date : "+dob);
                            
                              String Query3="select GEO_FENCE_CORDS, INOUT, ID from (select GEO_FENCE_CORDS, INOUT, to_char(EXPIRE_DATE,'yyyy-mm-dd') as sdf, ID from GEOFENCE_SETUP where VEHICLE_IMEI_NUMBER ='"+arr1[1]+"' and STATUS= '1') where sdf>='"+new java.sql.Date(dob.getTime())+"' ";   //and EXPIRE_DATE >= '"+dob.getTime()+"'
                              System.out.println("date "+new java.sql.Date(dob.getTime()));
                            System.out.println(Query3);
                            PreparedStatement ps13 = conn.prepareStatement(Query3);                                 
                            ResultSet rs3=ps13.executeQuery();
                            String cords=null;
                            String inout=null;
                            int geo_id;
                            //int k=0;
                            while (rs3.next()) {
                                 cords = rs3.getString("GEO_FENCE_CORDS");
                                 // store maximum value into total
                                 inout= rs3.getString("INOUT");
                                 geo_id=rs3.getInt("ID");
                                 
                                 //geofencetesting check2= new geofencetesting();
                                checkgeo(cords,location,arr1[1],String.valueOf(degree_to_decimal(arr[7],arr[8])),String.valueOf(degree_to_decimal(arr[9],arr[10])),inout,geo_id,datew);
                                
                            }
                            alarminsert(arr1[1],location,clientRequestMessage,datew);
                            //System.out.println("special"+k);
                            
                            
                            
                            //String[] cordsamount = cords.split(",");
                            
                            
                            String Query1="select MAX(ID) as insert_id from GPS_INFO_HISTORY";
                            PreparedStatement ps1 = conn.prepareStatement(Query1);
                            ResultSet rs=ps1.executeQuery();
                            int total=0;
                            while (rs.next()) {
                                 total = rs.getInt("insert_id"); // store maximum value into total
                            }
                            if("1".equals(arr[14])){
                                String speed="0";
                                 if(arr[11]==null||"".equals(arr[11])){
    
            }else{
                                 speed= arr[11];
            }   
                                
                            // insert all value insert into GPS_INFO_HISTORY table
                            String Query = "Insert into GPS_INFO_HISTORY(ID, SN_IMEI_ID, TRACKER_ID, L_DATETIME, R_DATETIME, LATITUDE, LONGITUDE, SPEED, ALTITUDE, AZIMUTH, HDOP, GPS_VALID, TRACKER_STATE, VOLTAGE1, VOLTAGE2, ALARM_ID, BASE_ID, SATELLITE_NUMBER, GSM_SIGNAL, JOURNEY, RUN_TIME, VOLTAGE3, VOLTAGE4, VOLTAGE5, VOLTAGE6, VOLTAGE7, VOLTAGE8, RFID, GEOFENCE_ALARM_ID, LOCATION, PROTOCOL_VER, PROTOCOL_DATA)  values"
                                    + "('"+Integer.toString(total+1)+"', "
                                    + "'"+arr1[1]+"', "
                                    + "'"+arr1[1]+"', "
                                    + "TO_DATE('"+datew+"', 'DD/MM/YYYY HH24:MI:SS'), "
                                    + "TO_DATE('"+datew+"', 'DD/MM/YYYY HH24:MI:SS'), "
                                    + "'"+String.valueOf(degree_to_decimal(arr[7],arr[8]))+"', "
                                    + "'"+String.valueOf(degree_to_decimal(arr[9],arr[10]))+"', "
                                    + "'"+Float.valueOf(speed)+"', "
                                    + "'"+0+"', "
                                    + "'"+0+"', "
                                    + "'"+0+"', "
                                    + "'"+1+"', "
                                    + "'"+1+"', "
                                    + "'"+0+"', "
                                    + "'"+0+"', "
                                    + "'"+123+"', "
                                    + null+", "
                                    + "'"+0+"', "
                                    + "'"+0+"', "
                                    + "'"+0+"', "
                                    + "'"+0+"', "
                                    + "'"+0+"', "
                                    + "'"+0+"', "
                                    + "'"+0+"', "
                                    + "'"+0+"', "
                                    + "'"+0+"', "
                                    + "'"+0+"', "
                                    + null+", "
                                    + "'"+0+"', "
                                    + "'"+location+"', "
                                    + "'"+1+"', "
                                    + null+")";
                            PreparedStatement ps = conn.prepareStatement(Query);
            //------ (ID, SN_IMEI_ID, TRACKER_ID,R_DATETIME, L_DATETIME,LATITUDE,LONGITUDE,SPEED, ALTITUDE, AZIMUTH, HDOP, TRACKER_STATE, GPS_VALID, ALARM_ID)
            
            // set date format datew variable
            
            //datew formation complete
            
            //Date date1=new SimpleDateFormat("dd/MM/yyyy").parse("02/15/2012");
//            ps.setInt(1, total+1);                                                     //ID
//            ps.setString(2, arr1[1]);                                                  //SN_IMEI_ID
//            ps.setString(3, arr1[1]);                                                  //TRACKER_ID
//            ps.setDate(4, new java.sql.Date(dob.getTime()));                           //R_DATETIME
//            ps.setDate(5, new java.sql.Date(dob.getTime()));                           //L_DATETIME
//            ps.setString(6, String.valueOf(degree_to_decimal(arr[7],arr[8])));         //LATITUDE
//            ps.setString(7, String.valueOf(degree_to_decimal(arr[9],arr[10])));        //LONGITUDE
//            if(arr[11]==null||"".equals(arr[11])){
//            ps.setFloat(8, 0);    
//            }else{
//            ps.setFloat(8, Float.valueOf(arr[11]));
//            }                                                                          //SPEED
//            ps.setInt(9, 0);                                                           //ALTITUDE
//            ps.setInt(10, 0);                                                          //AZIMUTH
//            ps.setFloat(11, 0);                                                        //HDOP
//            ps.setString(12, "1");                                                     //GPS_VALID
//            ps.setString(13, "1");                                                     //TRACKER_STATE
//            ps.setFloat(14, 0);                                                        //VOLTAGE1
//            ps.setFloat(15, 0);                                                        //VOLTAGE2
//            ps.setInt(16, 123); //ps.setInt(16, Integer.parseInt(arr[1]));             //ALARM_ID
//            ps.setString(17, null);                                                    //BASE_ID
//            ps.setInt(18, 0);                                                          //SATELLITE_NUMBER
//           ps.setInt(19, 0);                                                           //GSM_SIGNAL
//          ps.setInt(20, 0);                                                            //JOURNEY
//            ps.setInt(21, 0);                                                          //RUN_TIME
//            ps.setFloat(22, 0);                                                        //VOLTAGE3
//            ps.setFloat(23, 0);                                                        //VOLTAGE4
//            ps.setFloat(24, 0);                                                        //VOLTAGE5
//           ps.setFloat(25, 0);                                                         //VOLTAGE6
//            ps.setFloat(26, 0);                                                        //VOLTAGE7
//            ps.setFloat(27, 0);                                                        //VOLTAGE8
//            ps.setString(28, null);                                                    //RFID
//            ps.setInt(29, 0);                                                          //GEOFENCE_ALARM_ID
//            ps.setString(30, location);                                                    //LOCATION
//            ps.setInt(31, 1);                                                          //PROTOCOL_VER
//            ps.setString(32, null);                                                    //PROTOCOL_DATA
            int c = ps.executeUpdate();
            if(c==1){
                System.out.println("successfully inserted");
//                String queryi="update GPS_INFO_HISTORY SET  L_DATETIME=TO_DATE('"+datew+"', 'DD/MM/YYYY HH24:MI:SS') , R_DATETIME=TO_DATE('"+datew+"', 'DD/MM/YYYY HH24:MI:SS') WHERE ID= (SELECT MAX(ID) FROM GPS_INFO_HISTORY where SN_IMEI_ID='"+arr1[1]+"')";
//                PreparedStatement ps1i = conn.prepareStatement(queryi);
//                String queryii="update GPS_INFO_HISTORY_CURRENT SET  L_DATETIME=TO_DATE('"+datew+"', 'DD/MM/YYYY HH24:MI:SS') , R_DATETIME=TO_DATE('"+datew+"', 'DD/MM/YYYY HH24:MI:SS') where SN_IMEI_ID="+arr1[1]+"and L_DATETIME<=TO_DATE('"+datew+"', 'DD/MM/YYYY HH24:MI:SS') and R_DATETIME<=TO_DATE('"+datew+"', 'DD/MM/YYYY HH24:MI:SS')";
//                PreparedStatement ps1ii = conn.prepareStatement(queryii);
//                int d = ps1i.executeUpdate();
//                if(d==1){
//                    System.out.println("successfully inserted");
//                }
//                ps1ii.executeUpdate();
            }
           // imei:865011030518469,tracker,181223162404,,F,102404.00,A,2344.75462,N,09023.81367,E,0.391,0;
                            }   
                            String Query_insert = "Insert into GPS_INFO_HISTORY_ALL(IMEI, DATE_TIME, LOCATION, ENGINE_STATUS, SPEED) values('"+arr1[1]+"',TO_DATE('"+datew+"', 'DD/MM/YYYY HH24:MI:SS'),'"+location+"', '"+arr[14]+"','"+Float.valueOf(arr[11])+"')";
                            PreparedStatement psss = conn.prepareStatement(Query_insert);
                            psss.executeUpdate();
                    break;
                }
                else{
                sendData(dout, clientData.getBytes());
                }  
                    
                }
                
            } catch (ParseException ex) {
                Logger.getLogger(JavaApplication12.class.getName()).log(Level.SEVERE, null, ex);
            } catch (SQLException ex) {
                Logger.getLogger(JavaApplication12.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException e) {
                log("Error handling client# " + clientNumber + ": " + e);
            } finally {
                try {
                    //din.flush();
                    dout.flush();
                    din.close();
                    try {
                        conn.close();
                    } catch (SQLException ex) {
                        Logger.getLogger(JavaApplication12.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    //out.flush();
                    //out.close();
                    //pw.flush();
                    //pw.close();
                    socket.close();
                } catch (IOException ex) {
                    Logger.getLogger(JavaApplication12.class.getName()).log(Level.SEVERE, null, ex);
                }
                log("Connection with client# " + clientNumber + " closed");
            }
        }

        /**
         * Logs a simple message.  In this case we just write the
         * message to the server applications standard output.
         */
        private void log(String message) {
            System.out.println(message);
        }

public String doProcess(String message) throws SQLException
        {
            String return_val= null;
                System.out.println("Processing the Client Request..."+message);
//                if (message.equals("CLIENT_REQUEST:SEND_SYSTEM_TIME"))
//                {
//                        Date date = new Date(System.currentTimeMillis());
//                        return date.toString();
//                }
//                else
//                {
//                        return "SERVER-ERROR:INVALID_REQUEST";
//                }
		
                if(message.length()==26){
                    return "LOAD";
                }
                else if(message.length()==16){
                    return "ON";
                }
                else{
                      String [] arr = message.split(",");
                    
                    
                    if(arr.length>1){
                        if("tracker".equals(arr[1])){
                        
                         }
                        else{
                        String [] arr1 = arr[0].split(":");
                            if(arr1.length==2){
                                if("imei".equals(arr1[0])&& "1".equals(arr[14])){
                                   return_val=command_check(arr1[1]);
                                }
                            }
                        }
                    }
                    
                    //imei:865011030518469,tracker,,,L,,,5265,,2a9b,,,;
                    return return_val;
                }
        }
        
        public String command_check(String imei) throws SQLException{
            String return_val= null;
            //String return_val= "**,imei:"+imei+",101,10s";
            String Query1="select count(*) as count_command from COMMANDS where EXECUTE_STATUS='0' and VEHICLE_IMEI='"+imei+"'";
            PreparedStatement psl1 = conn.prepareStatement(Query1);
            ResultSet rs1=psl1.executeQuery();
            int total=0;
            while (rs1.next()) {
                total = rs1.getInt("count_command"); // store maximum value into total
            }
            if(total>0){
               
            String Query2="select * from COMMANDS where EXECUTE_STATUS='0' and VEHICLE_IMEI='"+imei+"' and ROWNUM=1";
            PreparedStatement ps12 = conn.prepareStatement(Query2);
            ResultSet rs2=ps12.executeQuery();
            
            while (rs2.next()) {
               int  command_type = rs2.getInt("COMMAND_ID");
               if(command_type==1){
                  return_val="**,imei:"+imei+",109";
            }
               else if(command_type==2){
                   return_val="**,imei:"+imei+",107,"+rs2.getInt("SPEED_LIMIT");
               }
               
              String Query = "update COMMANDS SET EXECUTE_STATUS=? WHERE ID=?";
              PreparedStatement ps = conn.prepareStatement(Query);
              ps.setInt(2, rs2.getInt("ID"));                                                  //TRACKER_ID
              ps.setInt(1, 1); 
              ps.executeUpdate();
                
                
            }
        }
            return return_val;
        }
        /**
         * Method receives the Client Request
         */
        public static byte[] receiveData(DataInputStream din) throws Exception
        {
                try
                {
                        byte[] inputData = new byte[1024];
                        din.read(inputData);
                        return inputData;
                }
                catch (Exception exception)
                {
                        throw exception;
                }
        }
        public static  boolean isInteger(String s) {
            boolean a= true;
    try { 
        Integer.parseInt(s); 
    } catch(NumberFormatException e) { 
        a= false; 
    } catch(NullPointerException e) {
        a= false;
    }
    // only got here if we didn't return false
    return a;
}
        public static String getAddressByGpsCoordinates(String lng, String lat)
            throws MalformedURLException, IOException, org.json.simple.parser.ParseException {
         
        URL url = new URL("https://maps.googleapis.com/maps/api/geocode/json?latlng="
                + lat + "," + lng + "&key=AIzaSyCXx3ubAosxwUQH4i4gMo6j89RaUbdISz0");
        //'https://maps.googleapis.com/maps/api/geocode/json?latlng=23.73929,90.375586&key=AIzaSyCXx3ubAosxwUQH4i4gMo6j89RaUbdISz0'
        HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
        String formattedAddress = "";
 
        try {
            InputStream in = url.openStream();
            
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String result, line = reader.readLine();
            result = line;
            System.out.println("hiii"+result);
            while ((line = reader.readLine()) != null) {
                result += line;
            }
 
            JSONParser parser = new JSONParser();
            JSONObject rsp = (JSONObject) parser.parse(result);
 
            if (rsp.containsKey("results")) {
                JSONArray matches = (JSONArray) rsp.get("results");
                JSONObject data = (JSONObject) matches.get(0); //TODO: check if idx=0 exists
                formattedAddress = (String) data.get("formatted_address");
                System.out.println("hi");
            }
            else{
                System.out.println("none");
            }
 
            return "";
        } finally {
            urlConnection.disconnect();
            return formattedAddress;
        }
    }

        /**
         * Method used to Send Response to Client
         */
        public static synchronized void sendData(DataOutputStream dout, byte[] byteData)
        {
                if (byteData == null)
                {
                        return;
                }
                try
                {
                        dout.write(byteData);
                        dout.flush();
                }
                catch (Exception exception)
                {
                }
        }
	public float degree_to_decimal(String coordinates_in_degrees, String direction){
            DecimalFormat df = new DecimalFormat("#0.######");
            int degrees = (int)(Float.valueOf(coordinates_in_degrees) / 100);
            double minutes = Double.valueOf(coordinates_in_degrees) - Double.valueOf(degrees * 100);
            double seconds = minutes / 60.0;
            double coordinates_in_decimal = degrees + seconds;
 
            if ((direction .equals("S")) || (direction.equals("W"))) {
                coordinates_in_decimal = coordinates_in_decimal * (-1);
            }
            String ret = df.format(coordinates_in_decimal);
            return  Float.valueOf(ret);
            //return (float) minutes;
        }
        
        public void alarminsert(String imei, String location, String s, String date_exact) throws SQLException{
            String alarm_id=null;
            String [] arr = s.split(",");
            if("help me".equals(arr[1])){
               alarm_id="006"; 
            }
            else if("low battery".equals(arr[1])){
               alarm_id="007"; 
            }
            else if("ac alarm".equals(arr[1])){
               alarm_id="009"; 
            }
            else if("speed".equals(arr[1])){
               alarm_id="003";
            }
            else if("acc on".equals(arr[1])||"1".equals(arr[14])){
               
              
               String Query_OF="select count(*) as count_extra from GPS_INFO_HISTORY_CURRENT WHERE SN_IMEI_ID = '"+imei+"' and L_DATETIME<=TO_DATE('"+date_exact+"','DD-MM-YYYY HH24:MI:SS')";
               PreparedStatement ps_OF = conn.prepareStatement(Query_OF);
                            ResultSet rs_OF=ps_OF.executeQuery();
                            int total_OF=0;
                            while (rs_OF.next()) {
                                 total_OF = rs_OF.getInt("count_extra"); // store maximum value into total
                            }
                            if(total_OF>0){
                                 alarm_id="001";
               String Query1="select count(*) as insert_id from ONOFF WHERE VEHICLE_IMEI = '"+imei+"'";
                            PreparedStatement ps1 = conn.prepareStatement(Query1);
                            ResultSet rs=ps1.executeQuery();
                            int total=0;
                            while (rs.next()) {
                                 total = rs.getInt("insert_id"); // store maximum value into total
                            }
                            if(total>0){
                                    String Query = "update ONOFF SET ONOFF=? WHERE VEHICLE_IMEI=? ";
                                    PreparedStatement ps = conn.prepareStatement(Query);
                                                                                     //SN_IMEI_ID
                                    ps.setString(2, imei);                                                  //TRACKER_ID
                                    ps.setInt(1, 1); 
                                     ps.executeUpdate();
                                    
                                
                            }
                            
                            String Queryyy = "SELECT ALARM_ID from ALARMS WHERE EXACT_TIME=TO_DATE((select MAX(TO_CHAR(EXACT_TIME,'DD-MM-YYYY HH24:MI:SS')) TIMEA from ALARMS WHERE (ALARM_ID = '001' OR ALARM_ID = '002') AND VEHICLE_IMEI='"+imei+"'),'DD-MM-YYYY HH24:MI:SS') and VEHICLE_IMEI='"+imei+"' AND (ALARM_ID = '001' OR ALARM_ID = '002')";
                       PreparedStatement psss = conn.prepareStatement(Queryyy);
                       ResultSet rsss=psss.executeQuery();
                       String alarmid=null;
                       while (rsss.next()) {
                                 alarmid = rsss.getString("ALARM_ID");
                                 if("002".equals(alarmid)){
                                     alarm_id="001";
                                 }
                                 else{
                                     alarm_id=null;
                                 }
                            }
                            }
               
            }
            else if("acc off".equals(arr[1])||"0".equals(arr[14])){
               
               String Query_OF="select count(*) as count_extra from GPS_INFO_HISTORY_CURRENT WHERE SN_IMEI_ID = '"+imei+"' and L_DATETIME<=TO_DATE('"+date_exact+"','DD-MM-YYYY HH24:MI:SS')";
               PreparedStatement ps_OF = conn.prepareStatement(Query_OF);
                            ResultSet rs_OF=ps_OF.executeQuery();
                            int total_OF=0;
                            while (rs_OF.next()) {
                                 total_OF = rs_OF.getInt("count_extra"); // store maximum value into total
                            }
                            if(total_OF>0){
                                alarm_id="002";
               String Query1="select count(*) as insert_id from ONOFF WHERE VEHICLE_IMEI = '"+imei+"'";
                            PreparedStatement ps1 = conn.prepareStatement(Query1);
                            ResultSet rs=ps1.executeQuery();
                            int total=0;
                            while (rs.next()) {
                                 total = rs.getInt("insert_id"); // store maximum value into total
                            }
                            if(total>0){
                                    String Query = "update ONOFF SET ONOFF=? WHERE VEHICLE_IMEI=? ";
                                    PreparedStatement ps = conn.prepareStatement(Query);
                                    ps.setString(2, imei);                                                  //TRACKER_ID
                                    ps.setInt(1, 0); 
                                     ps.executeUpdate();
                                    
                                
                            }
                            else{
                                 String Query = "insert into ONOFF (ONOFF,VEHICLE_IMEI) values (?,?)";
                                    PreparedStatement ps = conn.prepareStatement(Query);
                                                                                     //SN_IMEI_ID
                                    ps.setString(2, imei);                                                  //TRACKER_ID
                                    ps.setInt(1, 0); 
                                     ps.executeUpdate();
                                
                            }
                            
                       String Queryyy = "SELECT ALARM_ID from ALARMS WHERE EXACT_TIME=TO_DATE((select MAX(TO_CHAR(EXACT_TIME,'DD-MM-YYYY HH24:MI:SS')) TIMEA from ALARMS WHERE (ALARM_ID = '001' OR ALARM_ID = '002') AND VEHICLE_IMEI='"+imei+"'),'DD-MM-YYYY HH24:MI:SS') and VEHICLE_IMEI='"+imei+"' AND (ALARM_ID = '001' OR ALARM_ID = '002')";
                       PreparedStatement psss = conn.prepareStatement(Queryyy);
                       ResultSet rsss=psss.executeQuery();
                       String alarmid=null;
                       int a=0;
                       while (rsss.next()) {
                                 alarmid = rsss.getString("ALARM_ID");
                                 if("001".equals(alarmid)){
                                     alarm_id="002";
                                 }
                                 else{
                                     alarm_id=null;
                                 }
                                 
                            }
                            }
                       
                       
               
            }
            else if("door alarm".equals(arr[1])){
               alarm_id="011"; 
            }
            else if("accident alarm".equals(arr[1])){
               alarm_id="013"; 
            }
            
            if(alarm_id!=null){
             String Query = "Insert into ALARMS(ALARM_ID, VEHICLE_IMEI, LOCATION, EXACT_TIME, SEEN_STATUS)  values('"+alarm_id+"', '"+imei+"', '"+location+"', TO_DATE('"+date_exact+"', 'DD/MM/YYYY HH24:MI:SS'), '0')";
    
            PreparedStatement ps = conn.prepareStatement(Query);
    
        
//	//System.out.println(dateFormat.format(date));
//            ps.setString(1, alarm_id);                                                     //ID
//            ps.setString(2, imei);                                                  //SN_IMEI_ID
//            ps.setString(3, location); 
//            ps.setString(4, location); 
//            //TRACKER_ID
//            //ps.setDate(4, new java.sql.Date("2018-08-06") );                           //R_DATETIME
//            ps.setInt(5, 0); 
            
             int c = ps.executeUpdate();
            if(c==1){
                System.out.println("successfully alarm inserted");
            }
            }
            
            
        }
        
        
        
        
public void checkgeo(String strArray, String location, String imei, String lat, String lang, String inout,int geo_id, String date_exact ) throws SQLException {

String[]strArraynew = strArray.split(",");
System.out.println(strArray +"  "+ strArraynew.length );
int side= strArraynew.length;
double x[]= new double [side];
double y[]= new double[side];
double lengthtopoint[]=new double [side];
double sidelength[]=new double [side];
double angles[]=new double [side];
double xcheck=Double.parseDouble(lat);
double ycheck=Double.parseDouble(lang);

int m=0;
for(int i=0;i<side;i=i+2)
{
//System.out.println("Enter x for vertex "+i);
x[m]=Double.parseDouble(strArraynew[i]);
System.out.println("Enter x "+m+" for vertex "+x[m]);
y[m]=Double.parseDouble(strArraynew[i+1]);
System.out.println("Enter y "+m+" for vertex "+y[m]);
lengthtopoint[m]=Math.sqrt(((x[m]-xcheck)*(x[m]-xcheck))+((y[m]-ycheck)*(y[m]-ycheck)));
System.out.println("length of line joining given check point and vertex "+m+" is " +lengthtopoint[m]);
m++;
}
side=side/2;
for(int k=0;k<side-1;k++)
{   
sidelength[k]=Math.sqrt(((x[k+1]-x[k])*(x[k+1]-x[k]))+((y[k+1]-y[k])*(y[k+1]-y[k])));
//System.out.println("length of side "+k +(k+1)+ " is " +sidelength[k]);
}
sidelength[side-1]=Math.sqrt(((x[0]-x[side-1])*(x[0]-x[side-1]))+((y[0]-y[side-1])*(y[0]-y[side-1])));
//System.out.println("length of side "+(side-1)+"0 is " +sidelength[side-1]);

/*for(int w=0;w<side-1;w++)
{   
System.out.println("length  is " +sidelength[w]);
}*/
for(int l=0;l<side-1;l++)
{
angles[l] =((180/(Math.PI)))*Math.acos(((lengthtopoint[l]*lengthtopoint[l])+(lengthtopoint[l+1]*lengthtopoint[l+1])-(sidelength[l]*sidelength[l]))/(2*lengthtopoint[l]*lengthtopoint[l+1]));
//System.out.println("Angle= " +angles[l]); 
}

angles[side-1] =((180/(Math.PI)))*Math.acos(((lengthtopoint[side-1]*lengthtopoint[side-1])+(lengthtopoint[0]*lengthtopoint[0])-(sidelength[side-1]*sidelength[side-1]))/(2*lengthtopoint[side-1]*lengthtopoint[0]));    
//System.out.println("Angle= " +angles[side-1]);    
int returnnum;
double sum=0;
for(int z=0;z<side;z++)
{
sum=sum+angles[z];
}
System.out.println("sum "+sum);
if (sum==360)
{

returnnum=0;
}
else if(sum<360)
   
{
 
 returnnum=1;
}
else{
    returnnum=2;
    
}
if("OUT".equals(inout) && returnnum==1){
    System.out.println(" The point"+(xcheck)+","+(ycheck)+"lies outside the polygon");
    String Query="select count(*) as countval from ALARMS where ALARM_ID='004' AND VEHICLE_IMEI='"+imei+"' AND SEEN_STATUS='0' AND GEOFENCE_ID = '"+geo_id+"' ";
    PreparedStatement ps1 = conn.prepareStatement(Query);
                            ResultSet rs=ps1.executeQuery();
                            int total=0;
                            while (rs.next()) {
                                 total = rs.getInt("countval"); // store maximum value into total
                            }
                            
                            
     if(total==0){  
      Query = "Insert into ALARMS(ALARM_ID, VEHICLE_IMEI, LOCATION, SEEN_STATUS, GEOFENCE_ID, EXACT_TIME)  values('004', '"+imei+"', '"+location+"', 0, '"+String.valueOf(geo_id)+"', TO_DATE('"+date_exact+"', 'DD/MM/YYYY HH24:MI:SS'))";
    
    PreparedStatement ps = conn.prepareStatement(Query);
    
        
//	//System.out.println(dateFormat.format(date));
//            ps.setString(1, "004");                                                     //ID
//            ps.setString(2, imei);                                                  //SN_IMEI_ID
//            ps.setString(3, location);                                                  //TRACKER_ID
//            //ps.setDate(4, new java.sql.Date("2018-08-06") );                           //R_DATETIME
//            ps.setInt(4, 0);
//            ps.setString(5, String.valueOf(geo_id));
            
             int c = ps.executeUpdate();
            if(c==1){
                System.out.println("successfully alarm inserted");
            }
     }

}
else if("IN".equals(inout)&& returnnum==0){
     
System.out.println(" The  point"+(xcheck)+","+(ycheck) +"lies inside  polygon ");

    String Query="select count(*) as countval from ALARMS where ALARM_ID='005' AND VEHICLE_IMEI='"+imei+"' AND SEEN_STATUS='0' AND GEOFENCE_ID = '"+geo_id+"' ";
    PreparedStatement ps1 = conn.prepareStatement(Query);
                            ResultSet rs=ps1.executeQuery();
                            int total=0;
                            while (rs.next()) {
                                 total = rs.getInt("countval"); // store maximum value into total
                            }
                            
                            
     if(total==0){ 
       Query = "Insert into ALARMS(ALARM_ID, VEHICLE_IMEI, LOCATION, SEEN_STATUS, GEOFENCE_ID, EXACT_TIME)  values('005', '"+imei+"', '"+location+"', 0, '"+String.valueOf(geo_id)+"', TO_DATE('"+date_exact+"', 'DD/MM/YYYY HH24:MI:SS'))";

    PreparedStatement ps = conn.prepareStatement(Query);
    
        
//	//System.out.println(dateFormat.format(date));
//            ps.setString(1, "005");                                                     //ID
//            ps.setString(2, imei);                                                  //SN_IMEI_ID
//            ps.setString(3, location);                                                  //TRACKER_ID
//            //ps.setDate(4, new java.sql.Date("2018-08-06") );                           //R_DATETIME
//            ps.setInt(4, 0); 
//            ps.setString(5, String.valueOf(geo_id));
            
             int c = ps.executeUpdate();
            if(c==1){
                System.out.println("successfully alarm inserted");
            }
     }
}
    //return returnnum; 
          
}
//end function;


    }
}

