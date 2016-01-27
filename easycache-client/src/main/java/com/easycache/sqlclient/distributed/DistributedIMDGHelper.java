package com.easycache.sqlclient.distributed;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

import net.sf.cglib.beans.BeanGenerator;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.easycache.serialization.HazelcastObject;
import com.hazelcast.easycache.utility.IMDGString;
import com.easycache.sqlclient.BeanGeneratorFactory;
import com.easycache.sqlclient.HazelcastDatabaseMetaData;
import com.easycache.sqlclient.TableMetaData;
import com.easycache.sqlclient.load.IMDGNamingPolicy;

public class DistributedIMDGHelper {
	private static IMap<String, TableMetaData> distTableMetaData = null;
	private static IList<String> distTableNames = null;
	private static IMap<String, Boolean> distStats = null;
	private static int nodeNum = -1;
	private static boolean bSlaveNode = false;

	public static HazelcastInstance hazelcast = null;
	
	public DistributedIMDGHelper(String name){
		if((hazelcast = Hazelcast.getHazelcastInstanceByName(name)) == null){
			newHazelcastInstance(name);
		}
		distTableMetaData = hazelcast.getMap(IMDGString.META_DATA);
		distTableNames = hazelcast.getList(IMDGString.TABLE_NAME_LIST);
		distStats = hazelcast.getMap(IMDGString.DIST_SATUS);
	}

	public static void newHazelcastInstance(String name) {

		Config config = new XmlConfigBuilder().build();
		config.setInstanceName(name);
		hazelcast = Hazelcast.newHazelcastInstance(config);
	}

	public void raceToMaster() {
		IMap<String, Integer> numMap = hazelcast.getMap(IMDGString.NODE_NUM);
		synchronized (numMap) {// TODO does this synchronized work over distributed nodes?
			//zhaohui liu , there's distributed lock in hazelcast
			if (!numMap.containsKey(IMDGString.MASTER)) {
				numMap.put(IMDGString.MASTER, new Integer(0));
				bSlaveNode = false;
				setMasterMetaStat(false);// master node meta data not ready
				System.out.println("-->Master Node<--");
			} else {
				bSlaveNode = true;
				Integer newInt = new Integer(numMap.get(IMDGString.MASTER));
				numMap.set(IMDGString.MASTER, newInt + 1);
				System.out.println("-->Slave Node<--");
			}
		}
	}
	
	public static boolean isSlaveNode(){
		return bSlaveNode;
	}
	
	/**
	 * statistics of imdg node members
	 */
	public int initLoadPart() {
		Cluster cluster = hazelcast.getCluster();
		Set<Member> members = cluster.getMembers();
		Iterator<Member> it = members.iterator();
		Integer num = new Integer(0);
		int nodesNum = 0;
		System.out.println("-->initLoadPart");

		IMap<String, Integer> loadParMap = hazelcast.getMap(IMDGString.LOAD_PART);
		while(it.hasNext()){
			Member mem = it.next();
			InetAddress ia = mem.getSocketAddress().getAddress();
			loadParMap.put(ia.getHostAddress(), num);
			System.out.println("-->\t" + ia.getHostAddress() + "\t" + num);
			num++;
			nodesNum++;
//			Member localMem = cluster.getLocalMember();
//			InetAddress iaLocal = localMem.getInetSocketAddress().getAddress();
//			if( ia.equals( iaLocal)){System.out.println("this is master");}
		}
		return nodesNum;
	}
	
	/**
	 * @return number of nodes in the cluster
	 */
	public int sizeOfIMDG(){
		IMap<String, Integer> loadParMap = hazelcast.getMap(IMDGString.LOAD_PART);
		int num = loadParMap.size();
		return num<=0 ? initLoadPart() : num;
	}
	
	public void spreadMetaInfo(TableMetaData tableMetaData) throws Exception {
		String tableName = tableMetaData.getTableName();
		if(distTableMetaData.get(tableName) == null){
			distTableMetaData.set(tableName, tableMetaData);
			distTableNames.add(tableName);
		}
	}
	
	public IList<String> getTableNames(){
		return distTableNames;
	}
	
	public TableMetaData getTableMetaData(String tableName){
		return distTableMetaData.get(tableName);
		
	}
	
	public void localizeMetaInfo(BeanGeneratorFactory beanGeneratorFactory, HazelcastDatabaseMetaData hazelcastDatabaseMetaData){
		for(String tableName : distTableNames){
			BeanGenerator beanGenerator = new BeanGenerator();
	        beanGenerator.setNamingPolicy(new IMDGNamingPolicy(tableName));
	    	beanGenerator.setSuperclass(HazelcastObject.class);
	    	
	    	TableMetaData tableMetaData = distTableMetaData.get(tableName);

        	ArrayList<String> columnNameList = tableMetaData.getColumnNameList();
        	ArrayList<Class> columnClassList = tableMetaData.getColumnClassList();        	
	        for (int i = 0; i < tableMetaData.getColumnSize(); i++) {
	        	beanGenerator.addProperty(columnNameList.get(i), columnClassList.get(i));
	        }
	        
	        beanGeneratorFactory.setBeanGenerator(tableName, beanGenerator);
	        hazelcastDatabaseMetaData.setTableMetaData(tableName, tableMetaData);
		}
	}
	
	public boolean isMasterMetaReady(){
		Boolean bStat = distStats.get(IMDGString.MASTER_READY);
		//System.out.println("distStats : "+ IMDGString.strMasterReady+ " : \t"  + bStat +"");
		return bStat==null ? false : bStat;
	}
	
	public boolean isAllGeneClassReady(){
		Boolean bStat = distStats.get(IMDGString.ALL_GENER_CLASS_READY);
		//System.out.println("distStats : "+ IMDGString.strMasterReady+ " : \t"  + bStat +"");
		return bStat== null ? false : bStat;
	}
	
	public void setAllGeneClassStat(boolean bStat){
		distStats.set(IMDGString.ALL_GENER_CLASS_READY, bStat);
	}
	
	
	public void setMasterMetaStat(boolean bStat){
		distStats.put(IMDGString.MASTER_READY, bStat);
		//System.out.println("distStats : "+ IMDGString.strMasterReady+ " : \t" + bStat +"");
	}
	
	/**
	 * @return number of node in the cluster, 0,1,2,...
	 */
	public static int getCurNodeNum() {
		if(nodeNum < 0){
			IMap<String, Integer> loadParMap = hazelcast.getMap(IMDGString.LOAD_PART);
			Cluster cluster = hazelcast.getCluster();
			nodeNum = loadParMap.get(cluster.getLocalMember().getSocketAddress().getAddress().getHostAddress());
			System.out.println("The local node number is " + nodeNum + " !");
		}
		return nodeNum;
	}
}
