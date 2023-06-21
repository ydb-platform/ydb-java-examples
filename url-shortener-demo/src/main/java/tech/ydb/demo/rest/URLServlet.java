package tech.ydb.demo.rest;

import java.io.IOException;
import java.io.InputStreamReader;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonWriter;

import tech.ydb.demo.Application;
import tech.ydb.demo.ydb.UrlRecord;
import tech.ydb.demo.ydb.YdbException;
import tech.ydb.demo.ydb.YdbRepository;

/**
 *
 * @author Alexandr Gorshenin
 */
public class URLServlet extends HttpServlet {
    private static final long serialVersionUID = -1410806003624620851L;

    private YdbRepository repository() {
        return new YdbRepository(Application.ydp());
    }

    @Override
    public void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        JsonElement json = JsonParser.parseReader(new InputStreamReader(req.getInputStream()));
        if (json == null || !json.isJsonObject() || !json.getAsJsonObject().has("source")) {
            resp.sendError(400, "unreadable request");
            return;
        }

        String source = json.getAsJsonObject().get("source").getAsString();
        UrlRecord record = new UrlRecord(source);

        try {
            repository().insertRecord(record);
        } catch (YdbException e) {
            throw new ServletException(e.getMessage(), e);
        }

        try (JsonWriter writer = new JsonWriter(resp.getWriter())) {
            writer.beginObject();
            writer.name("hash").value(record.hash());
            writer.endObject();
        }
    }
}
