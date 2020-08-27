import lxml.etree
import xml2json

from custom.icds.form_processor.aadhaar_number_extractor import AadhaarNumberExtractor


def pre_process_form(instance):
    instance_xml = _get_instance_xml(instance)
    if instance_xml:
        instance_xml = AadhaarNumberExtractor().run(instance_xml)
        instance = lxml.etree.tostring(instance_xml)
    return instance


def _get_instance_xml(instance):
    try:
        return xml2json.get_xml_from_string(instance)
    except xml2json.XMLSyntaxError:
        return None
