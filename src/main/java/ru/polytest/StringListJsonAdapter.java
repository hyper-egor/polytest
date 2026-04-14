package ru.polytest;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class StringListJsonAdapter extends TypeAdapter<List<String>> {
    private static final Gson GSON = new Gson();
    private static final TypeToken<List<String>> LIST_OF_STRING_TYPE = new TypeToken<List<String>>() {};

    @Override
    public void write(JsonWriter out, List<String> value) throws IOException {
        if (value == null) {
            out.nullValue();
            return;
        }

        GSON.toJson(value, LIST_OF_STRING_TYPE.getType(), out);
    }

    @Override
    public List<String> read(JsonReader in) throws IOException {
        JsonToken token = in.peek();

        if (token == JsonToken.NULL) {
            in.nextNull();
            return Collections.emptyList();
        }

        if (token == JsonToken.BEGIN_ARRAY) {
            return GSON.fromJson(in, LIST_OF_STRING_TYPE.getType());
        }

        if (token == JsonToken.STRING) {
            String rawValue = in.nextString();
            if (rawValue == null || rawValue.isBlank()) {
                return Collections.emptyList();
            }
            return GSON.fromJson(rawValue, LIST_OF_STRING_TYPE.getType());
        }

        throw new IOException("Unsupported token for string list: " + token);
    }
}
