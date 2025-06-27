package de.bschwering.edi_workshop.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Edition {
    String key;
    String name;
    String description;
}
