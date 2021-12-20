package ru.romanow.stream.processor.models;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class BookPart {
    private String name;
    private String text;
}
