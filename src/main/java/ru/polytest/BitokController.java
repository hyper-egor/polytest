package ru.polytest;


import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.*;

@RestController
@RequestMapping("/")
public class BitokController {
    
    private static final Logger log = LoggerFactory.getLogger(BitokController.class);
    
   @Autowired
   StakanService stakanService;

    @RequestMapping(value = "/x", method = RequestMethod.GET)
    public String root()
    {
        String rv = "<html><body><ul>"
                +"<li><a href='work'>work</a></li>"
                +"<li><a href='autoChangeAmount'>autoChangeAmount</a></li>"
                +"<li><a href='mincandles'>mincandles</a></li>"
                +"<li><a href='profitKoof'>profitKoof</a></li>"
                +"<li><a href='maxDeals'>maxDeals</a></li>"
                +"<li><a href='minDelay'>minDelay</a></li>"
                +"<li><a href='tradeAmnt'>maxTradeAmnt</a></li>"
                +"<li><a href='closeAll'>closeAll</a></li>"
                +"</ul></body></html>";
        return rv;
    }
    
    
}