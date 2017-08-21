package com.compass.hadoop.mr.sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 实现 WritableComparable接口，序列化+自定义排序功能
 */
public class InfoBean implements WritableComparable<InfoBean>{

	private String account;// 账号
	private double income;// 收入
	private double expenses;// 支出
	private double surplus;// 结余
	
	// 不使用构造方法，需要new Bean，模仿hadoop的结构，添加set方法
	public void set(String account, double income, double expenses){
		this.account = account;
		this.income = income;
		this.expenses = expenses;
		this.surplus = income - expenses;
	}
	
	/**
	 * org.apache.hadoop.io.Writable 接口：序列化，注意与反序列化的顺序和类型
	 */
	public void write(DataOutput out) throws IOException {
		out.writeUTF(account);
		out.writeDouble(income);
		out.writeDouble(expenses);
		out.writeDouble(surplus);
	}
	/**
	 * org.apache.hadoop.io.Writable 接口：反序列化，注意与序列化的顺序和类型
	 */
	public void readFields(DataInput in) throws IOException {
		this.account = in.readUTF(); 
		this.income = in.readDouble(); 
		this.expenses = in.readDouble(); 
		this.surplus = in.readDouble(); 
	}

	/**
	 * java.lang.Comparable 接口：排序
	 */
	public int compareTo(InfoBean o) {
		if(this.income == o.getIncome()) { // 收入相等
			return this.expenses > o.getExpenses() ? 1 : -1; // 则比较支出
		}
		return this.income > o.getIncome() ? 1 : -1;
	}

	
	public String getAccount() {
		return account;
	}
	public void setAccount(String account) {
		this.account = account;
	}
	public double getIncome() {
		return income;
	}
	public void setIncome(double income) {
		this.income = income;
	}
	public double getExpenses() {
		return expenses;
	}
	public void setExpenses(double expenses) {
		this.expenses = expenses;
	}
	public double getSurplus() {
		return surplus;
	}
	public void setSurplus(double surplus) {
		this.surplus = surplus;
	}

	@Override
	public String toString() {
		return  income + "\t" +	expenses + "\t" + surplus; // 收入	支出		结余
	}
}
