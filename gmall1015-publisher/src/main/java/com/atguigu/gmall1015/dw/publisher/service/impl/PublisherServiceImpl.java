package com.atguigu.gmall1015.dw.publisher.service.impl;

import com.atguigu.gmall1015.dw.common.constant.GmallConstant;
import com.atguigu.gmall1015.dw.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    JestClient jestClient;

    /**
     * 查询日活总数
     * @param date
     * @return
     */
    @Override
    public Integer getDauTotal(String date) {
        String queryDsl = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"logDate\": \""+date+"\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_DEFAULT_TYPE).build();
        Integer total=0;
        try {
            SearchResult searchResult = jestClient.execute(search);
              total = searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return total;
    }

    @Override
    public Map getDauMap(String date) {
        Map dauMap=new HashMap();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate",date));
        searchSourceBuilder.query(boolQueryBuilder);
        //聚合
        TermsBuilder termsAggsBuilder = AggregationBuilders.terms("groupby_logHour").field("logHour").size(24);
        searchSourceBuilder.aggregation(termsAggsBuilder);

        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_DAU).addType(GmallConstant.ES_DEFAULT_TYPE).build();

        try {
            SearchResult searchResult = jestClient.execute(search);
            //得到聚合的统计结果
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                dauMap.put( bucket.getKey(),bucket.getCount());
            }


        } catch (IOException e) {
            e.printStackTrace();
        }


        return dauMap;
    }

    /**
     * 求当日总交易额
     * @param date
     * @return
     */
    @Override
    public Double getOrderTotalAmount(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合
        SumBuilder sumAgg = AggregationBuilders.sum("sum_totalAmount").field("totalAmount");
        searchSourceBuilder.aggregation(sumAgg);

        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_NEW_ORDER).addType(GmallConstant.ES_DEFAULT_TYPE).build();
        Double totalAmount=0D;
        try {
            SearchResult searchResult = jestClient.execute(search);
            totalAmount = searchResult.getAggregations().getSumAggregation("sum_totalAmount").getSum();


        } catch (IOException e) {
            e.printStackTrace();
        }
        return totalAmount;
    }


    /**
     * 当日分时交易额
     * @param date
     * @return
     */
    @Override
    public Map getOrderTotalAmountHourMap(String date) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //过滤
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("createDate",date));
        searchSourceBuilder.query(boolQueryBuilder);

        //聚合
        TermsBuilder termAggs = AggregationBuilders.terms("groupby_createHour").field("createHour").size(24);
        //sum子聚合
        SumBuilder sumAgg = AggregationBuilders.sum("sum_totalAmount").field("totalAmount");
        termAggs.subAggregation(sumAgg);

        searchSourceBuilder.aggregation(termAggs);

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_NEW_ORDER).addType(GmallConstant.ES_DEFAULT_TYPE).build();

        Map<String,Double> orderMap=new HashMap<>();
        try {
            SearchResult searchResult = jestClient.execute(search);
            //取聚合后的sum结果
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_createHour").getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                orderMap.put( bucket.getKey(),bucket.getSumAggregation("sum_totalAmount").getSum());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


        return orderMap;
    }

    @Override
    public Map getSaleDetail(String date, String keyword, int startPage, int pagesize, String aggField, int aggSize) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //过滤部分  1 日期过滤  2 关键词匹配
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("dt",date));
        boolQueryBuilder.must(new MatchQueryBuilder("sku_name",keyword).operator(MatchQueryBuilder.Operator.AND));

        searchSourceBuilder.query(boolQueryBuilder);

        //聚合 按给定的字段进行聚合
        TermsBuilder aggsGender = AggregationBuilders.terms("groupby_"+aggField).field(aggField).size(aggSize);
        searchSourceBuilder.aggregation(aggsGender);

        searchSourceBuilder.from((startPage-1)*pagesize);
        searchSourceBuilder.size(pagesize);
        System.out.println(searchSourceBuilder.toString());

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(GmallConstant.ES_INDEX_SALE_DETAIL).addType(GmallConstant.ES_DEFAULT_TYPE).build();

        //  查询结果： 1明细 2聚合结果 3 总数  =》 Map

         List<HashMap> detailList = new ArrayList();
         Map<String,Long> aggMap=new HashMap();
         int total=0;

         Map<String,Object> saleInfoMap=new HashMap();
        try {
            SearchResult searchResult = jestClient.execute(search);
            List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
            for (SearchResult.Hit<HashMap, Void> hit : hits) {
                detailList.add(hit.source) ;
            }
            List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_" + aggField).getBuckets();
            for (TermsAggregation.Entry bucket : buckets) {
                aggMap.put(bucket.getKey(),bucket.getCount());
            }
            total = searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }

        saleInfoMap.put("detail",detailList);
        saleInfoMap.put("aggMap",aggMap);
        saleInfoMap.put("total",total);
        return saleInfoMap;
    }
}
