namespace NifiKit.SiteToSite;

/// <summary>
///   Site-to-Site protocol response codes, mirroring
///   org.apache.nifi.remote.protocol.ResponseCode. Names are kept identical to
///   NiFi's so they can be matched against NiFi logs and documentation.
/// </summary>
public enum SiteToSiteResponseCode {
  RESERVED = 0,
  PROPERTIES_OK = 1,
  CONTINUE_TRANSACTION = 10,
  FINISH_TRANSACTION = 11,
  CONFIRM_TRANSACTION = 12,
  TRANSACTION_FINISHED = 13,
  TRANSACTION_FINISHED_BUT_DESTINATION_FULL = 14,
  CANCEL_TRANSACTION = 15,
  BAD_CHECKSUM = 19,
  MORE_DATA = 20,
  NO_MORE_DATA = 21,
  UNKNOWN_PORT = 200,
  PORT_NOT_IN_VALID_STATE = 201,
  PORTS_DESTINATION_FULL = 202,
  UNKNOWN_PROPERTY_NAME = 230,
  ILLEGAL_PROPERTY_VALUE = 231,
  MISSING_PROPERTY = 232,
  UNAUTHORIZED = 240,
  ABORT = 250,
  UNRECOGNIZED_RESPONSE_CODE = 254,
  END_OF_STREAM = 255
}
