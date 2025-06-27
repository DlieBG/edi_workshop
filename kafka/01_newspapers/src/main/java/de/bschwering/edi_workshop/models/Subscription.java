package de.bschwering.edi_workshop.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Subscription {
    String id;
    String personName;
    String editionKey;
}
