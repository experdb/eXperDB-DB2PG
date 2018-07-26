package test.write;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import com.k4m.experdb.db2pg.common.Constant;

/** tmp Class ( volatilizable ) 
 * maybe this class is removed as soon.*/
public final class TestFileWriter implements Closeable {

	private boolean stop = true;
	private boolean running = false;
	private boolean open = false;
	private boolean close = true;
	private File file; 
	private ByteBuffer byteBuffer;
	private FileOutputStream fileOutputStream;
	private FileChannel fileChannel;
	private String charset;
	
	public static void main (String[] args) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader("C:\\test\\test.txt"));
		TestFileWriter tfw = new TestFileWriter("C:\\test\\test2.out", "UTF-8", 16);
		
		String lineStr = null;
		while( (lineStr=br.readLine()) != null) {
			tfw.write(lineStr);
		}
		tfw.close();
		tfw = null;
		
		br.close();
	}
	
	public TestFileWriter(String file, String charset, int directBufferCapacity) {
		this(new File(file), charset, directBufferCapacity);
	}
	
	public TestFileWriter(File file, String charset, int directBufferCapacity) {
		this.file = file;
		this.byteBuffer = ByteBuffer.allocateDirect(directBufferCapacity);
		this.byteBuffer.clear();
		this.charset = charset;
		try {
			this.fileOutputStream = new FileOutputStream(file,false);
			this.fileOutputStream.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public boolean write(char[] str) {
		return write(new String(str));
	}
	
	public boolean write(String str) {
		try {
			open();
			
			byte[] bytes = str.getBytes(charset);
			
			int bfsCnt = bytes.length/byteBuffer.capacity();
			System.out.println(">>"+bytes.length+"/"+byteBuffer.capacity());
			for(int i=0;i<bfsCnt;i++) {
				byteBuffer.put(Arrays.copyOfRange(bytes, i*byteBuffer.capacity(), (i+1)*byteBuffer.capacity()));
				System.out.print(String.format("%d/%d\t", i*byteBuffer.capacity(), (i+1)*byteBuffer.capacity()));
				byteBuffer.flip();
				fileChannel.write(byteBuffer);
				byteBuffer.clear();
			}
			
			if(bytes.length%byteBuffer.capacity()>0) {
				byteBuffer.put(Arrays.copyOfRange(bytes, bfsCnt*byteBuffer.capacity(), bytes.length));
				System.out.print(String.format("%d/%d\t", bfsCnt*byteBuffer.capacity(), bytes.length));
			}
			byteBuffer.put(Constant.R.getBytes());
			byteBuffer.flip();
			fileChannel.write(byteBuffer);
			byteBuffer.clear();
			System.out.println();
			System.out.println();
//			for(int i=0;i<bfsCnt;i++) {
//				String sub = null;
//				if(i<bfsCnt-1) {
//					sub = str.substring(i*byteBuffer.capacity(), (i+1)*byteBuffer.capacity());
//				} else {
//					sub = str.substring(i*byteBuffer.capacity(), i*byteBuffer.capacity()+(str.length()%byteBuffer.capacity()));
//				}
//				if(sub != null){
//					if(byteBuffer.capacity()<sub.getBytes(charset).length)
//					System.out.println(byteBuffer.capacity()+"/"+sub.getBytes(charset).length+"/"+sub+"/"+(i<bfsCnt-1));
//					byteBuffer.put(sub.getBytes(charset));
//					byteBuffer.flip();
//	    			
//					fileChannel.write(byteBuffer);
//	    			byteBuffer.clear();
//				}
//			}
			close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public void stop() throws IOException {
		close();
		stop=true;
	}
	
	public boolean isRunning() {
		return running;
	}
	
	public boolean isOpen() {
		return this.open;
	}
	
	public boolean isClose() {
		return this.close;
	}
	
	public void open() throws FileNotFoundException {
		if (!isOpen()) { 
			this.fileOutputStream = new FileOutputStream(file,true);
			this.fileChannel = this.fileOutputStream.getChannel();
			this.open = true;
			this.close = false;
		}
	}

	public void close() throws IOException {
		if (!isClose()) {
			byteBuffer.clear();
			fileChannel.close();
			fileOutputStream.close();
			this.open = false;
			this.close = true;
		}
	}

}
