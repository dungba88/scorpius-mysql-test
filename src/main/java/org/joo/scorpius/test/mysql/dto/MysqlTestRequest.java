package org.joo.scorpius.test.mysql.dto;

import org.joo.scorpius.support.BaseRequest;

import lombok.Getter;

@Getter
public class MysqlTestRequest extends BaseRequest {

    private static final long serialVersionUID = 4736263882514253351L;
    
    private String name;
}
