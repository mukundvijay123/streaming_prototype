package serviceb.utils;

public class context{
    public final String JWTToken;
    public final String action;
    
    public context(String token,String action){
        this.JWTToken=token;
        this.action=action;
    }
}
