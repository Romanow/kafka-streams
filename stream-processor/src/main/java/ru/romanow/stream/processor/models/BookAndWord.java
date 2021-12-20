package ru.romanow.stream.processor.models;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class BookAndWord {
    private String book;
    private String word;
}
