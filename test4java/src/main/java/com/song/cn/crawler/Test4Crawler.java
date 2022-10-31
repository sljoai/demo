package com.song.cn.crawler;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

public class Test4Crawler {
    public static void main(String[] args) throws IOException {
        // https://www.hkinventory.com/p/d/DP83867IRPAP.htm
//        String url="https://www.hkinventory.com/p/d/DP83867IRPAP.html";
        String url = "https://www.hkinventory.com/Portlets/Portlet_InstantNewsUpdateCheck.asp?XML=1&LastNewsID=52673483";
        /*Document document = Jsoup.parse(new URL(url), 30000);
        Element divNewsElement = document.getElementById("divNews");
        Elements allElements = divNewsElement.getElementsByTag("a");
        for (Element element:allElements){
            System.out.println(element.getElementsByTag("span"));
        }*/
        URL url1 = new URL(url);
        HttpURLConnection urlConnection = (HttpURLConnection) url1.openConnection();
        urlConnection.setRequestMethod("GET");
        urlConnection.setReadTimeout(15000);
        urlConnection.connect();
        if(urlConnection.getResponseCode()==200){
            InputStream is = urlConnection.getInputStream();
            if(is!=null){
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is,"UTF-8"));
                StringBuilder result = new StringBuilder();
                String temp=null;
                while ((temp= bufferedReader.readLine())!=null){
                    result.append(temp);
                }
                System.out.println(result.toString());
            }
        }
    }
}
