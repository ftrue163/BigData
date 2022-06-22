package com.atguigu.gmallpublisher1.service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface ProductService {
    BigDecimal getGmv(int date);

    Map getGmvByTm(int date, int limit);
}
