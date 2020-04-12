package com.ywzngk.web.kettle;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.KettleLogLayout;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.KettleLoggingEvent;
import org.pentaho.di.core.logging.KettleLoggingEventListener;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingBuffer;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.job.JobEntryJob;
import org.pentaho.di.job.entries.trans.JobEntryTrans;
import org.pentaho.di.job.entry.JobEntryCopy;
import org.pentaho.di.job.entry.JobEntryInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ywzngk.utils.ResourceUtils;
import com.ywzngk.utils.YwzngkConstant;

public class RunKettleJob {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(RunKettleJob.class);
    public static void main(String[] args) {
        String kjbPath = "C:\\Users\\Administrator\\Desktop\\_data\\Job_entrance.kjb";
        Map<String, String> params = new HashMap<>();
        params.put("date", "20200224");
//        String ktrPath = "D:\\ProgramFiles\\data-integration\\_data\\预警配置\\02-贷款异动预警\\41-网点贷款异动预警-日-百分比.ktr";
//        runs(kjbPath);
//        runTrans(params, ktrPath);
//            runJob(params, kjbPath);
        runJobOnce(params, kjbPath);
//        runRepoJob(params);
    }
    private static Map<String,String> getsuffixFile(String dir) {
        File path = new File(dir);
        Map<String,String> map= new HashMap<>();
        if(path.exists()){
            File[] listFiles = path.listFiles();
            String absolutePath = null;
            for(File f : listFiles){
                absolutePath = f.getAbsolutePath();
                if(f.isDirectory()){
                    map.putAll(getsuffixFile(absolutePath));
                }else{
                        map.put(f.getName(), absolutePath);
                }
            }
        }
        return map;
    }
    private static Map<String,String> getsuffixFile(String dir,String suffix) {
		File path = new File(dir);
		Map<String,String> map= new HashMap<>();
		if(path.exists()){
			File[] listFiles = path.listFiles();
			String absolutePath = null;
			for(File f : listFiles){
				absolutePath = f.getAbsolutePath();
				if(f.isDirectory()){
					map.putAll(getsuffixFile(absolutePath,suffix));
				}else{
					if(absolutePath.endsWith(suffix)){
						map.put(f.getName(), absolutePath);
					}
				}
			}
		}
		return map;
	}
    public static void resetTransDirOfJob(String kjbPath,JobMeta jobMeta,Map<String,String> parammap){

    	try{
            List<JobEntryCopy> jobCopies = jobMeta.getJobCopies();
//            boolean ischanged=false;
            String rsfilename = null;
            for(JobEntryCopy o : jobCopies){
                JobEntryInterface entry = o.getEntry();
                	String filename = entry.getFilename();
                if(filename!=null&&!filename.startsWith(YwzngkConstant.kettledir)&&!filename.startsWith("${")){
                	if(!filename.startsWith("${")){
                		filename=filename.substring(filename.lastIndexOf(File.separator)+1);
                		if(filename.contains("/")){
                			filename=filename.substring(filename.lastIndexOf("/")+1);
                		}
                		rsfilename=parammap.get(filename);
                		LOGGER.info("{}作业引入的转换或作业{}的路径是：{}",kjbPath,filename,rsfilename);
                	}
//                	else if(filename.startsWith("${")){
//                		String exactFilename = jobMeta.environmentSubstitute( entry.getFilename() );
//                		if(exactFilename!=null){
//                			rsfilename=exactFilename;
//                		}
//                		LOGGER.info("{}作业引入的转换或作业{}的exact的路径是：{}",kjbPath,filename,rsfilename);
//                	}
                        if(entry instanceof JobEntryTrans ){
                            ((JobEntryTrans)entry).setFileName(rsfilename);
                        }else if(entry instanceof JobEntryJob){
                            ((JobEntryJob)entry).setFileName(rsfilename);
                        }
//                		ischanged=true;
                	}
                }
//            if(ischanged){
//            	writeXML(kjbPath,jobMeta.getXML());
//            }
        } catch (Exception e) {
        	LOGGER.error("作业设置路径出错：",e);
        }
    
    
    }
    public static void runs(String kjbPath){

        try {
            if(!KettleEnvironment.isInitialized()){
                KettleEnvironment.init();
            }
            JobMeta jobMeta = new JobMeta(kjbPath, null);
            jobMeta.environmentSubstitute("");
            List<JobEntryCopy> jobCopies = jobMeta.getJobCopies();

            for(JobEntryCopy o : jobCopies){

                JobEntryInterface entry = o.getEntry();
                if(entry instanceof JobEntryTrans){
                    ((JobEntryTrans)entry).setFileName("123412341234");
                }
            }
            LOGGER.info(jobMeta.getXML());
            
            jobMeta.setChanged();
            writeXML(kjbPath,jobMeta.getXML());
        } catch (Exception e) {
        	LOGGER.error("作业执行出错：",e);
        }
    
    }
    public static void writeXML(String filename,String xml) throws KettleXMLException {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(filename);
            fos.write(XMLHandler.getXMLHeader().getBytes("UTF-8"));
            fos.write(xml.getBytes("UTF-8"));
        } catch (Exception var11) {
            throw new KettleXMLException("Unable to save to XML file '" + filename + "'", var11);
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException var10) {
                    throw new KettleXMLException("Unable to close file '" + filename + "'", var10);
                }
            }

        }

    }
    /**
     * 运行资源库中的作业
     *
     * @param params 作业参数
     */
//    public static void runRepoJob(Map<String, String> params) {
//        try {
//            KettleEnvironment.init();
//             repository = new KettleDatabaseRepository();
//            // 配置资源库数据库连接信息
//            DatabaseMeta databaseMeta = new DatabaseMeta(
//                    "kettle",KettleDatabaseRepository
//                    "mysql",
//                    "jdbc",
//                    "127.0.0.1",
//                    "kettle",
//                    "3308",
//                    "root",
//                    "lwsjfwq"
//            );
//            // 配置连接参数，指定连接编码为UTF8，若不指定则不能读取中文目录或者中文名作业
//            databaseMeta.getAttributes().put("EXTRA_OPTION_MYSQL.characterEncoding", "utf8");
//            // 连接测试
//            if (databaseMeta.testConnection().startsWith("正确")) {
//                System.out.println("数据库连接成功");
//            } else {
//                System.out.println("数据库连接失败");
//                return;
//            }
//            // 配置资源库
//            KettleDatabaseRepositoryMeta repositoryMeta = new KettleDatabaseRepositoryMeta(
//                    "kettle",
//                    "kettle",
//                    "Kettle Repository",
//                    databaseMeta
//            );
//            repository.init(repositoryMeta);
//            // 连接资源库
//            repository.connect("admin", "admin");
//            // 指定job或者trans所在的目录
//            RepositoryDirectoryInterface dir = repository.findDirectory("/批处理/");
//            // 选择资源库中的作业
//            JobMeta jobMeta = repository.loadJob("资源库作业示例", dir, null, null);
//            // 配置作业参数
//            for (String param : params.keySet()) {
//                jobMeta.setParameterValue(param, params.get(param));
//            }
//            Job job = new Job(repository, jobMeta);
//            job.setLogLevel(LogLevel.DEBUG);
//            //执行作业
//            job.start();
//            //等待作业执行结束
//            job.waitUntilFinished();
//            if (job.getErrors() > 0) {
//                throw new Exception("作业执行出错");
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
    /**
     * 运行转换文件
     *
     * @param params  转换参数
     * @param ktrPath 转换文件的路径，后缀ktr
     */
    public static void runTrans(Map<String, String> params, String ktrPath) {
        try {
            // 初始化
            runkettleinit();
            TransMeta transMeta = new TransMeta(ktrPath);
            // 配置参数
            for (Map.Entry<String,String> param : params.entrySet()) {
                
                transMeta.setParameterValue(param.getKey(), param.getValue());
                transMeta.setVariable(param.getKey(), param.getValue());
            }
            Trans trans = new Trans(transMeta);

            // 设置日志级别
            trans.setLogLevel(LogLevel.BASIC);
            // 执行转换
            trans.execute(null);
            // 等待转换执行结束
            trans.waitUntilFinished();
//            logtxt(ktrPath, trans.getLogChannelId(), trans.getResult(), "\ntrans %s executed with result: %s and %d errors\n");
            // 抛出异常
            if (trans.getErrors() > 0) {
                throw new Exception("转换执行出错");
            }
        } catch (Exception e) {
        	LOGGER.error("转换文件出错",e);
        }
    }
    /**
     * 运行作业文件
     *
     * @param params  作业参数
     * @param kjbPath 作业文件路径，后缀kjb
     */
    public static Job runJob(Map<String, String> params, String kjbPath) {
		if (null != kjbPath) {

			Job job = null;
			try {
				runkettleinit();

				JobMeta jobMeta = new JobMeta(kjbPath, null);
				if (params != null) {
					// 配置作业参数
					for (Map.Entry<String, String> param : params.entrySet()) {
						jobMeta.setParameterValue(param.getKey(), param.getValue());
						jobMeta.setVariable(param.getKey(), param.getValue());
					}
				}
				resetTransDirOfJob(kjbPath, jobMeta, getsuffixFile(YwzngkConstant.kettledir));
				// 配置变量
				job = new Job(null, jobMeta);
				LOGGER.info("job的name是：{}", job.getJobname());
				// 设置日志级别
				job.setLogLevel(LogLevel.ROWLEVEL);
				job.setName(job.getJobname());
				// 启动作业
				job.start();
				if (job.getErrors() > 0) {
					throw new Exception("作业执行出错");
				}
			} catch (Exception e) {
				LOGGER.error("作业执行出错：", e);
			}
			return job;
		}
		return null;
    }
	private static void runkettleinit() throws KettleException {
		if(!KettleEnvironment.isInitialized()){
			LOGGER.info("KettleEnvironment初始化成功");
			String classPath = ResourceUtils.getClassPath();
			LOGGER.info(classPath);
			
	        String pluginfolder = classPath.substring(0, classPath.lastIndexOf("classes"))+"kettle-json-plugin";
	        LOGGER.info("pluginfolder是：{}",pluginfolder);
			System.setProperty("KETTLE_PLUGIN_BASE_FOLDERS",pluginfolder);
			LOGGER.info("KETTLE_PLUGIN_BASE_FOLDERS是：{}",System.getProperty("KETTLE_PLUGIN_BASE_FOLDERS"));
            System.setProperty("KETTLE_HOME",YwzngkConstant.kettledir);
		    KettleEnvironment.init();
//		    System.getProperties().remove("vfs.sftp.userDirIsRoot");
		    LoggingBuffer loggingBuffer = KettleLogStore.getAppender();
		    loggingBuffer.addLoggingEventListener(new KettleLoggingEventListener() {
		    	private KettleLogLayout layout = new KettleLogLayout( true );
				@Override
				public void eventAdded(KettleLoggingEvent arg0) {
					String txt = layout.format( arg0 );
					LOGGER.info(txt);
				}
			});
		}
	}
    /**
     * 运行作业文件执行一次
     *
     * @param params  作业参数
     * @param kjbPath 作业文件路径，后缀kjb
     */
    /**
     * @param params
     * @param kjbPath
     */
    public static void runJobOnce(Map<String, String> params, String kjbPath) {
    	try {
    		runkettleinit();
    		JobMeta jobMeta = new JobMeta(kjbPath, null);

            for (Map.Entry<String,String> param : params.entrySet()) {
                jobMeta.setParameterValue(param.getKey(), param.getValue());
                jobMeta.setVariable(param.getKey(), param.getValue());
            }
            JobEntryCopy start = jobMeta.getStart();
            JobEntryCopy findNextJobEntry = jobMeta.findNextJobEntry(start, 0);
            // 配置变量
            resetTransDirOfJob(kjbPath,jobMeta,getsuffixFile(YwzngkConstant.kettledir));
            Job job = new Job(null, jobMeta);
            LOGGER.info("单次作业从{}开始",findNextJobEntry);
            job.setStartJobEntryCopy(findNextJobEntry);
            // 配置作业参数

    		// 设置日志级别
    		job.setLogLevel(LogLevel.BASIC);
    		// 启动作业
    		job.start();
    		// 等待作业执行完毕
    		job.waitUntilFinished();
//            logtxt(kjbPath, job.getLogChannelId(), job.getResult(), "\njob %s executed with result: %s and %d errors\n");
            if (job.getErrors() > 0) {
    			throw new Exception("作业执行出错");
    		}
    	} catch (Exception e) {
    		LOGGER.error("作业执行出错：{}",e.getMessage(),e);
    	}
    }
}