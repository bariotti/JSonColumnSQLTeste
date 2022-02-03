import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.sql.*;
import java.util.Locale;
import java.util.Random;

public class JSonColumnTeste {

    Connection conn;

    public static void main(String[] args) throws SQLException, ClassNotFoundException {

        JSonColumnTeste teste = new JSonColumnTeste();

        /*for(int i = 1; i<=1000; i++)
        {
            Mensagem msg = teste.gerarMensagem();
            String json = teste.converterMensagemToJsonString(msg);
            teste.insert("FX", json);
        }*/

        ResultSet rs = teste.select();
        teste.convertResultSetToJson(rs);
    }

    private void convertResultSetToJson(ResultSet rs) throws SQLException, ClassNotFoundException {
        JsonArray array = new JsonArray();
        while(rs.next())
        {
            int colunas = rs.getMetaData().getColumnCount();
            JsonObject obj = new JsonObject();
            for(int i = 0;i<colunas;i++)
            {
                String nomeElemento = rs.getMetaData().getColumnName(i+1).toLowerCase();
                rs.getObject(i+1);
                JsonObject elemento = (JsonObject) rs.getObject(i+1);
                obj.add(nomeElemento, elemento);

                //obj.add(rs.getMetaData().getColumnName(i+1).toLowerCase(), (JsonElement) rs.getObject(i + 1));
            }
            array.add(obj);
        }
    }

    private Mensagem gerarMensagem()
    {
        Mensagem msg = new Mensagem();
        msg.Origem = "OE5";

        Random random = new Random();
        int valor = random.nextInt((1000 - 1) + 1) + 1;

        msg.Conta = String.valueOf(valor);
        msg.Valor = Double.valueOf(valor);
        msg.Identificador = "AGE" + valor;

        return msg;
    }

    private String converterMensagemToJsonString(Mensagem msg)
    {
        Gson gson = new Gson();
        return gson.toJson(msg);
    }

    private void insert(String produto, String mensagem) throws SQLException {

        try
        {
            this.getConnection();
            Statement comando = conn.createStatement();
            comando.execute("insert into testejson (data_criaca, produto, mensagem) values (getdate(), '" + produto + "', '" + mensagem + "')");
        }
        catch(Exception ex)
        {
            System.out.println("ERRO: " + ex.getMessage());
        }
        finally
        {
            if(!conn.isClosed())
            conn.close();
        }
    }

    private Connection getConnection()
    {
        try
        {
            if(conn == null || conn.isClosed())
            {
                String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
                Class.forName(driver);
                conn = DriverManager.getConnection("jdbc:sqlserver://DESKTOP-B1QQIIN\\SQLEXPRESS;database=teste;integratedSecurity=true");
            }
        }
        catch(Exception ex)
        {
            System.out.println("ERRO: " + ex.getMessage());
        }

        return conn;
    }

    private ResultSet select() throws SQLException {

        getConnection();
        String sql = "select * from testejson";
        Statement comando = conn.createStatement();
        ResultSet rs = comando.executeQuery(sql);

        return rs;
    }
}
