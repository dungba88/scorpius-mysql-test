package org.joo.scorpius.test.mysql;

import org.joo.scorpius.support.BaseResponse;

import lombok.Getter;

@Getter
public class MysqlTestResponse extends BaseResponse {

    private static final long serialVersionUID = -2348893603511565914L;

    private final User[] users;
    
    public MysqlTestResponse(User[] users) {
        this.users = users;
    }
}
