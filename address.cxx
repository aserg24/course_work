#encoding "utf8"

StreetW -> 'проспект' | 'проезд' | 'улица' | 'шоссе';
StreetSokr -> 'пр' | 'просп' | 'пр-д' | 'ул' | 'ш';


StreetDescr -> StreetW | StreetSokr;

StreetNameNoun -> (Adj<gnc-agr[1]>) Word<gnc-agr[1], rt> (Word<gram="род">);

NumberW_1 -> AnyWord<wff=/[1-9]?[0-9]-?((ый)|(ий)|(ой)|й)/> {outgram="муж, ед, им"};
NumberW_2 -> AnyWord<wff=/[1-9]?[0-9]-?((ая)|(яя)|(ья)|я)/> {outgram="жен, ед, им"};
NumberW_3 -> AnyWord<wff=/[1-9]?[0-9]-?((ее)|(ье)|(ое)|е)/> {outgram="сред, ед, им"};

NumberW -> NumberW_1 | NumberW_2 | NumberW_3;

StreetNameAdj -> Adj<h-reg1> Adj*;
StreetNameAdj -> NumberW<gnc-agr[1]> Adj<gnc-agr[1]>;

StreetNumber -> AnyWord<wff=/[1-9][0-9]?[0-9]?[А-Я]?/>;


Street -> StreetDescr interp (Address.Descr) StreetNameNoun<gram="род", h-reg1> interp (Address.StreetName::not_norm);
Street -> StreetDescr interp (Address.Descr) StreetNameNoun<gram="им", h-reg1> interp (Address.StreetName::not_norm);
Street -> StreetNameAdj<gnc-agr[1]> interp (Address.StreetName) StreetW<gnc-agr[1]> interp (Address.Descr);
Street -> StreetNameAdj interp (Address.StreetName) StreetSokr interp (Address.Descr);
Street -> StreetW<gnc-agr[1]> interp (Address.Descr) StreetNameAdj<gnc-agr[1]> interp (Address.StreetName);
Street -> StreetSokr interp (Address.Descr) StreetNameAdj interp (Address.StreetName);
Address -> Street StreetNumber interp (Address.Number);





















