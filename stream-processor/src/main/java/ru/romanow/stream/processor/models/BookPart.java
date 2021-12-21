package ru.romanow.stream.processor.models;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class BookPart {
    private String name;
    private String text;

    public static BookPart create(String name, String text) {
        return new BookPart()
                .setName(name)
                .setText(text);
    }
}
