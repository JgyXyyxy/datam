package com.sdu.inas.datam;

import com.sdu.inas.datam.service.Mongo2Hbase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DatamApplication implements CommandLineRunner{

	@Autowired
	Mongo2Hbase mongo2Hbase;

	public static void main(String[] args) {
		SpringApplication.run(DatamApplication.class, args);
	}


	@Override
	public void run(String... strings) throws Exception {

		mongo2Hbase.transData();
		System.out.println("运行结束");

	}
}
