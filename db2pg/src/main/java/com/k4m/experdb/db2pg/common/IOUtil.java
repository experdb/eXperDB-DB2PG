package com.k4m.experdb.db2pg.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.StringTokenizer;
import java.util.Vector;


public class IOUtil {
	public static boolean ExistsFile(String sFile){
		File fFile = new File( sFile );
		return fFile.exists();
	}
	/************************************************************
	 * READ FILE
	 ************************************************************/
	public static String readFile( String sFile, String sEndOfLine ){
		
		BufferedReader br = null;
		StringBuffer sContents = new StringBuffer();

		try{
			File fFile = new File( sFile );
			br = new BufferedReader ( new FileReader( fFile ));
			
			String sRead = null;
			while( ( sRead = br.readLine() ) != null ) {
				sContents.append(sRead).append(sEndOfLine);
				//sContents = sContents + sRead + sEndOfLine;
			}
		}catch( Exception ex ) {
			LogUtils.error(ex.toString(), IOUtil.class);
			LogUtils.error(sFile+" couldn't be found", IOUtil.class);
			return null;
		}finally {
			try{
				if( br != null ) br.close();
			}catch( Exception exx ) {}
		}		
		//Log.err( 0, new IOUtil(), sFile+" was read" );
		return sContents.toString();
	}
	
	/************************************************************
	 * READ FILE
	 ************************************************************/
	public static Vector<String[]> readFile( String sFile, String delim, int elementCnt ){
		
		BufferedReader br = null;
		Vector<String[]> vcContents = new Vector<String[]>();
		String[] aryLine = null;

		try{
			File fFile = new File( sFile );
			br = new BufferedReader ( new FileReader( fFile ) );
			
			String sRead = null;
			while( ( sRead = br.readLine() ) != null ) {
				aryLine = new String[ elementCnt ];
				StringTokenizer st = new StringTokenizer( sRead, delim );
				for( int i=0; st.hasMoreElements(); i++ ){
					aryLine[i] = st.nextToken();
				}
				vcContents.add( aryLine );
			}
		}catch( Exception ex ) {
			LogUtils.error(ex.toString(), IOUtil.class);
			LogUtils.error(sFile+" couldn't be found", IOUtil.class);
			return null;
		}finally {
			try{
				if( br != null ) br.close();
			}catch( Exception exx ) {}
		}		
		LogUtils.error(sFile+" was read", IOUtil.class);
		return vcContents; 
	}		
	
	/************************************************************
	 * RETURN FILE VECTOR IN SPECIFIEC DIRECTORY
	 ************************************************************/
	public static Vector<File> getFiles( String sDir, String sFileExt ){
	
		Vector<File> vcFile = new Vector<File>();

		try{
			File dataFileDir = new File( sDir );
			File[] fileAry = dataFileDir.listFiles();
			if( fileAry != null && fileAry.length > 0 ){
				for( int i=0; i<fileAry.length; i++ ){
					if( fileAry[i].getName().toLowerCase().endsWith( sFileExt ) ){
						LogUtils.error(fileAry[i].getName() + " was found", IOUtil.class);
						vcFile.add( fileAry[i] );
					}
				}
			}
		}catch( Exception ex ) {
			ex.printStackTrace();
		}
		
		return vcFile;
	}	
	
	/************************************************************
	 * RETURN FILE VECTOR IN SPECIFIEC DIRECTORY
	 ************************************************************/
	public static File getFile( String sFile ){
	
		File fFile = null;
		try{
			fFile = new File( sFile );
		}catch( Exception ex ) {
			ex.printStackTrace();
		}
		
		return fFile;
	}		
		
	/************************************************************
	 * CREATE DIRECTORY
	 ************************************************************/
	public static boolean createDir( String sDir ){
		try{
			if( !new File( sDir ).exists() ){
				new File( sDir ).mkdir();
				LogUtils.info(sDir+" was created", IOUtil.class);
			}			
		}catch( Exception ex ) {
			ex.printStackTrace();
		}
		
		if( !new File( sDir ).exists() ){
			LogUtils.error(sDir+" couldn't be created", IOUtil.class);
			return false;
		}
		
		return true;
	}

	/************************************************************
	 * CREATE FILE
	 ************************************************************/
	public static PrintWriter createFile( String sDir, String sFile ){
		createDir( sDir );
		PrintWriter pw = null;

		try{
			if( !sDir.endsWith("/") ){ sDir = sDir + "/"; }
			pw = new PrintWriter ( new FileWriter ( new File( sDir+sFile ), true) );
			LogUtils.info(sDir+sFile+" was created", IOUtil.class);
		}catch( Exception ex ) {
			ex.printStackTrace();
		}
		
		if( !new File( sDir+sFile ).exists() ){
			LogUtils.error(sDir+sFile+" couldn't be created", IOUtil.class);
			return null;
		}
		
		return pw;
	}
	
	/************************************************************
	 * CREATE FILE
	 ************************************************************/
	public static PrintWriter createPipeFile( String sDir, String sFile ){
		createDir( sDir );
		PrintWriter pw = null;

		try{
			if( !sDir.endsWith("/") ){ sDir = sDir + "/"; }
			pw = new PrintWriter ( new OutputStreamWriter  ( new FileOutputStream( sDir+sFile ) ) );		
			LogUtils.info(sDir+sFile+" was created", IOUtil.class);
		}catch( Exception ex ) {
			ex.printStackTrace();
		}
		
		if( !new File( sDir+sFile ).exists() ){
			LogUtils.error(sDir+sFile+" couldn't be created", IOUtil.class);
			return null;
		}
		
		return pw;
	}
	
	/************************************************************
	 * CREATE FILE
	 ************************************************************/
	public static void createFile( String sDir, String sFileName, String sContent ){
		if( !IOUtil.createDir( sDir ) ) System.exit(0);
		PrintWriter pw = IOUtil.createFile( sDir, sFileName );
		try{
			if( pw != null ){
				pw.write( new String( sContent ) );
				pw.flush();
			}
			LogUtils.info(sDir+sFileName+" was created", IOUtil.class);
		}catch( Exception ex ) {
			ex.printStackTrace();
		}
	}
	
	/************************************************************
	 * CREATE FILE
	 ************************************************************/
	public static void WriteFile( File file, byte[] sContent ){
		FileOutputStream fos = null;
		try{
			fos = new FileOutputStream(file);
			
			if( fos != null ){
				fos.write( sContent );
				fos.flush();
				fos.close();
			}
		}catch( Exception ex ) {
			ex.printStackTrace();
		}
	}
	
	/************************************************************
	 * CREATE FILE
	 ************************************************************/
	public static byte[] ReadFile( File file){
		FileInputStream fis = null;
		int length = (int)file.length();
		byte[] bContents = new byte[length];
		try{
			fis = new FileInputStream(file);
			
			if( fis != null ){
				fis.read(bContents);
				fis.close();
			}

			return bContents;
		}catch( Exception ex ) {
			ex.printStackTrace();
			return null;
		}
	}
	
	/************************************************************
	 * DELETE FILE
	 ************************************************************/
	public static void deleteFile( String sDir, String sFileName ){
		File file = new File( sDir+sFileName );
		if( file.exists() ){
			file.delete();
			LogUtils.info(sDir+sFileName+" was deleted", IOUtil.class);
		}else{
			LogUtils.error(sDir+sFileName+" couldn't be found and deleted", IOUtil.class);
		}
	}
	
	/************************************************************
	 * RUN OS COMMAND
	 ************************************************************/
	public static String runOSCommand( String[] aryCommand ){
		long lStartTime = System.currentTimeMillis();
		long lEndTime = 0;
		try{
			String sCommand = "";
			for( int i=0; i<aryCommand.length; i++ ){
				if( aryCommand[i] == null || aryCommand[i].equals( "null") ){
					aryCommand[i] = "";
				}
				sCommand += aryCommand[i] + " ";
			}
			LogUtils.info(sCommand, IOUtil.class);
			Runtime.getRuntime().exec( aryCommand , null, null);
			//Process proc = Runtime.getRuntime().exec( aryCommand );
			Thread.sleep( 100 );
			/*
			BufferedReader br = new BufferedReader( new InputStreamReader( proc.getInputStream() ) );
			String sRead = null;
			while( ( sRead = br.readLine()) != null ){
				Log.err( 0, new IOUtil(), "sRead:"+sRead );
			}
			lEndTime = System.currentTimeMillis();
			*/
			lEndTime = System.currentTimeMillis();
		}catch( Exception ex ){
			ex.printStackTrace();
		}
		return String.valueOf( (double)(lEndTime-lStartTime)/(double)1000 );
	}
	
	public static String getCurrentDir() {
		String curDir = System.getProperty("user.dir");
		return curDir;
	}
}
