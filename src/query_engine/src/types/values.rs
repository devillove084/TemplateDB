use std::cmp::Ordering;
use std::fmt;
use std::hash::Hash;
use std::iter::repeat;
use std::sync::Arc;

use arrow::array::{
    new_null_array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder, Date32Array,
    Date32Builder, Float32Array, Float32Builder, Float64Array, Float64Builder, Int16Array,
    Int16Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, Int8Array, Int8Builder,
    IntervalDayTimeArray, IntervalDayTimeBuilder, IntervalMonthDayNanoBuilder,
    IntervalYearMonthArray, IntervalYearMonthBuilder, StringArray, StringBuilder, UInt16Array,
    UInt16Builder, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder, UInt8Array,
    UInt8Builder,
};
use arrow::datatypes::{DataType, IntervalUnit};
use ordered_float::OrderedFloat;

use super::{LogicalType, TypeError};

#[derive(Clone)]
pub enum ScalarValue {
    Null,
    Boolean(Option<bool>),
    Float32(Option<f32>),
    Float64(Option<f64>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Utf8(Option<String>),
    /// Date stored as a signed 32bit int days since UNIX epoch 1970-01-01
    Date32(Option<i32>),
    /// Number of elapsed whole months
    IntervalYearMonth(Option<i32>),
    /// Number of elapsed days and milliseconds (no leap seconds)
    /// stored as 2 contiguous 32-bit signed integers
    IntervalDayTime(Option<i64>),
}

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        use ScalarValue::*;
        match (self, other) {
            (Boolean(v1), Boolean(v2)) => v1.eq(v2),
            (Boolean(_), _) => false,
            (Float32(v1), Float32(v2)) => {
                let v1 = v1.map(OrderedFloat);
                let v2 = v2.map(OrderedFloat);
                v1.eq(&v2)
            }
            (Float32(_), _) => false,
            (Float64(v1), Float64(v2)) => {
                let v1 = v1.map(OrderedFloat);
                let v2 = v2.map(OrderedFloat);
                v1.eq(&v2)
            }
            (Float64(_), _) => false,
            (Int8(v1), Int8(v2)) => v1.eq(v2),
            (Int8(_), _) => false,
            (Int16(v1), Int16(v2)) => v1.eq(v2),
            (Int16(_), _) => false,
            (Int32(v1), Int32(v2)) => v1.eq(v2),
            (Int32(_), _) => false,
            (Int64(v1), Int64(v2)) => v1.eq(v2),
            (Int64(_), _) => false,
            (UInt8(v1), UInt8(v2)) => v1.eq(v2),
            (UInt8(_), _) => false,
            (UInt16(v1), UInt16(v2)) => v1.eq(v2),
            (UInt16(_), _) => false,
            (UInt32(v1), UInt32(v2)) => v1.eq(v2),
            (UInt32(_), _) => false,
            (UInt64(v1), UInt64(v2)) => v1.eq(v2),
            (UInt64(_), _) => false,
            (Utf8(v1), Utf8(v2)) => v1.eq(v2),
            (Utf8(_), _) => false,
            (Null, Null) => true,
            (Null, _) => false,
            (Date32(v1), Date32(v2)) => v1.eq(v2),
            (Date32(_), _) => false,
            (IntervalYearMonth(v1), IntervalYearMonth(v2)) => v1.eq(v2),
            (IntervalYearMonth(_), _) => false,
            (IntervalDayTime(v1), IntervalDayTime(v2)) => v1.eq(v2),
            (IntervalDayTime(_), _) => false,
        }
    }
}

impl PartialOrd for ScalarValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use ScalarValue::*;
        match (self, other) {
            (Boolean(v1), Boolean(v2)) => v1.partial_cmp(v2),
            (Boolean(_), _) => None,
            (Float32(v1), Float32(v2)) => {
                let v1 = v1.map(OrderedFloat);
                let v2 = v2.map(OrderedFloat);
                v1.partial_cmp(&v2)
            }
            (Float32(_), _) => None,
            (Float64(v1), Float64(v2)) => {
                let v1 = v1.map(OrderedFloat);
                let v2 = v2.map(OrderedFloat);
                v1.partial_cmp(&v2)
            }
            (Float64(_), _) => None,
            (Int8(v1), Int8(v2)) => v1.partial_cmp(v2),
            (Int8(_), _) => None,
            (Int16(v1), Int16(v2)) => v1.partial_cmp(v2),
            (Int16(_), _) => None,
            (Int32(v1), Int32(v2)) => v1.partial_cmp(v2),
            (Int32(_), _) => None,
            (Int64(v1), Int64(v2)) => v1.partial_cmp(v2),
            (Int64(_), _) => None,
            (UInt8(v1), UInt8(v2)) => v1.partial_cmp(v2),
            (UInt8(_), _) => None,
            (UInt16(v1), UInt16(v2)) => v1.partial_cmp(v2),
            (UInt16(_), _) => None,
            (UInt32(v1), UInt32(v2)) => v1.partial_cmp(v2),
            (UInt32(_), _) => None,
            (UInt64(v1), UInt64(v2)) => v1.partial_cmp(v2),
            (UInt64(_), _) => None,
            (Utf8(v1), Utf8(v2)) => v1.partial_cmp(v2),
            (Utf8(_), _) => None,
            (Null, Null) => Some(Ordering::Equal),
            (Null, _) => None,
            (Date32(v1), Date32(v2)) => v1.partial_cmp(v2),
            (Date32(_), _) => None,
            (IntervalYearMonth(v1), IntervalYearMonth(v2)) => v1.partial_cmp(v2),
            (IntervalYearMonth(_), _) => None,
            (IntervalDayTime(v1), IntervalDayTime(v2)) => v1.partial_cmp(v2),
            (IntervalDayTime(_), _) => None,
        }
    }
}

impl Eq for ScalarValue {}

impl Hash for ScalarValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        use ScalarValue::*;
        match self {
            Boolean(v) => v.hash(state),
            Float32(v) => {
                let v = v.map(OrderedFloat);
                v.hash(state)
            }
            Float64(v) => {
                let v = v.map(OrderedFloat);
                v.hash(state)
            }
            Int8(v) => v.hash(state),
            Int16(v) => v.hash(state),
            Int32(v) => v.hash(state),
            Int64(v) => v.hash(state),
            UInt8(v) => v.hash(state),
            UInt16(v) => v.hash(state),
            UInt32(v) => v.hash(state),
            UInt64(v) => v.hash(state),
            Utf8(v) => v.hash(state),
            Null => 1.hash(state),
            Date32(v) => v.hash(state),
            IntervalYearMonth(v) => v.hash(state),
            IntervalDayTime(v) => v.hash(state),
        }
    }
}

macro_rules! typed_cast {
    ($array:expr, $index:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        let array = $array.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        ScalarValue::$SCALAR(match array.is_null($index) {
            true => None,
            false => Some(array.value($index).into()),
        })
    }};
}

macro_rules! build_array_from_option {
    ($DATA_TYPE:ident, $ARRAY_TYPE:ident, $EXPR:expr, $SIZE:expr) => {{
        match $EXPR {
            Some(value) => Arc::new($ARRAY_TYPE::from_value(*value, $SIZE)),
            None => new_null_array(&DataType::$DATA_TYPE, $SIZE),
        }
    }};
    ($DATA_TYPE:ident, $ENUM:expr, $ARRAY_TYPE:ident, $EXPR:expr, $SIZE:expr) => {{
        match $EXPR {
            Some(value) => Arc::new($ARRAY_TYPE::from_value(*value, $SIZE)),
            None => new_null_array(&DataType::$DATA_TYPE($ENUM), $SIZE),
        }
    }};
    ($DATA_TYPE:ident, $ENUM:expr, $ENUM2:expr, $ARRAY_TYPE:ident, $EXPR:expr, $SIZE:expr) => {{
        match $EXPR {
            Some(value) => {
                let array: ArrayRef = Arc::new($ARRAY_TYPE::from_value(*value, $SIZE));
                // Need to call cast to cast to final data type with timezone/extra param
                cast(&array, &DataType::$DATA_TYPE($ENUM, $ENUM2)).expect("cannot do temporal cast")
            }
            None => new_null_array(&DataType::$DATA_TYPE($ENUM, $ENUM2), $SIZE),
        }
    }};
}

impl ScalarValue {
    pub fn new_none_value(data_type: &DataType) -> Result<Self, TypeError> {
        match data_type {
            DataType::Null => Ok(ScalarValue::Null),
            DataType::Boolean => Ok(ScalarValue::Boolean(None)),
            DataType::Float32 => Ok(ScalarValue::Float32(None)),
            DataType::Float64 => Ok(ScalarValue::Float64(None)),
            DataType::Int8 => Ok(ScalarValue::Int8(None)),
            DataType::Int16 => Ok(ScalarValue::Int16(None)),
            DataType::Int32 => Ok(ScalarValue::Int32(None)),
            DataType::Int64 => Ok(ScalarValue::Int64(None)),
            DataType::UInt8 => Ok(ScalarValue::UInt8(None)),
            DataType::UInt16 => Ok(ScalarValue::UInt16(None)),
            DataType::UInt32 => Ok(ScalarValue::UInt32(None)),
            DataType::UInt64 => Ok(ScalarValue::UInt64(None)),
            DataType::Utf8 => Ok(ScalarValue::Utf8(None)),
            other => Err(TypeError::NotImplementedArrowDataType(other.to_string())),
        }
    }

    /// Converts a value in `array` at `index` into a ScalarValue
    pub fn try_from_array(array: &ArrayRef, index: usize) -> Result<Self, TypeError> {
        if !array.is_valid(index) {
            return Self::new_none_value(array.data_type());
        }

        use arrow::array::*;

        Ok(match array.data_type() {
            DataType::Null => ScalarValue::Null,
            DataType::Boolean => typed_cast!(array, index, BooleanArray, Boolean),
            DataType::Float64 => typed_cast!(array, index, Float64Array, Float64),
            DataType::Float32 => typed_cast!(array, index, Float32Array, Float32),
            DataType::UInt64 => typed_cast!(array, index, UInt64Array, UInt64),
            DataType::UInt32 => typed_cast!(array, index, UInt32Array, UInt32),
            DataType::UInt16 => typed_cast!(array, index, UInt16Array, UInt16),
            DataType::UInt8 => typed_cast!(array, index, UInt8Array, UInt8),
            DataType::Int64 => typed_cast!(array, index, Int64Array, Int64),
            DataType::Int32 => typed_cast!(array, index, Int32Array, Int32),
            DataType::Int16 => typed_cast!(array, index, Int16Array, Int16),
            DataType::Int8 => typed_cast!(array, index, Int8Array, Int8),
            DataType::Utf8 => typed_cast!(array, index, StringArray, Utf8),
            other => {
                return Err(TypeError::NotImplementedArrowDataType(other.to_string()));
            }
        })
    }

    pub fn get_logical_type(&self) -> LogicalType {
        match self {
            ScalarValue::Null => LogicalType::SqlNull,
            ScalarValue::Boolean(_) => LogicalType::Boolean,
            ScalarValue::Float32(_) => LogicalType::Float,
            ScalarValue::Float64(_) => LogicalType::Double,
            ScalarValue::Int8(_) => LogicalType::Tinyint,
            ScalarValue::Int16(_) => LogicalType::Smallint,
            ScalarValue::Int32(_) => LogicalType::Integer,
            ScalarValue::Int64(_) => LogicalType::Bigint,
            ScalarValue::UInt8(_) => LogicalType::UTinyint,
            ScalarValue::UInt16(_) => LogicalType::USmallint,
            ScalarValue::UInt32(_) => LogicalType::UInteger,
            ScalarValue::UInt64(_) => LogicalType::UBigint,
            ScalarValue::Utf8(_) => LogicalType::Varchar,
            ScalarValue::Date32(_) => LogicalType::Date,
            ScalarValue::IntervalYearMonth(_) => LogicalType::Interval(IntervalUnit::YearMonth),
            ScalarValue::IntervalDayTime(_) => LogicalType::Interval(IntervalUnit::DayTime),
        }
    }

    /// Converts a scalar value into an 1-row array.
    pub fn to_array(&self) -> ArrayRef {
        self.to_array_of_size(1)
    }

    /// Converts a scalar value into an array of `size` rows.
    pub fn to_array_of_size(&self, size: usize) -> ArrayRef {
        match self {
            ScalarValue::Boolean(e) => Arc::new(BooleanArray::from(vec![*e; size])) as ArrayRef,
            ScalarValue::Float64(e) => {
                build_array_from_option!(Float64, Float64Array, e, size)
            }
            ScalarValue::Float32(e) => {
                build_array_from_option!(Float32, Float32Array, e, size)
            }
            ScalarValue::Int8(e) => build_array_from_option!(Int8, Int8Array, e, size),
            ScalarValue::Int16(e) => build_array_from_option!(Int16, Int16Array, e, size),
            ScalarValue::Int32(e) => build_array_from_option!(Int32, Int32Array, e, size),
            ScalarValue::Int64(e) => build_array_from_option!(Int64, Int64Array, e, size),
            ScalarValue::UInt8(e) => build_array_from_option!(UInt8, UInt8Array, e, size),
            ScalarValue::UInt16(e) => {
                build_array_from_option!(UInt16, UInt16Array, e, size)
            }
            ScalarValue::UInt32(e) => {
                build_array_from_option!(UInt32, UInt32Array, e, size)
            }
            ScalarValue::UInt64(e) => {
                build_array_from_option!(UInt64, UInt64Array, e, size)
            }

            ScalarValue::Utf8(e) => match e {
                Some(value) => Arc::new(StringArray::from_iter_values(repeat(value).take(size))),
                None => new_null_array(&DataType::Utf8, size),
            },
            ScalarValue::Null => new_null_array(&DataType::Null, size),
            ScalarValue::Date32(e) => {
                build_array_from_option!(Date32, Date32Array, e, size)
            }
            ScalarValue::IntervalDayTime(e) => build_array_from_option!(
                Interval,
                IntervalUnit::DayTime,
                IntervalDayTimeArray,
                e,
                size
            ),
            ScalarValue::IntervalYearMonth(e) => build_array_from_option!(
                Interval,
                IntervalUnit::YearMonth,
                IntervalYearMonthArray,
                e,
                size
            ),
        }
    }

    pub fn new_builder(data_type: &LogicalType) -> Result<Box<dyn ArrayBuilder>, TypeError> {
        match data_type {
            LogicalType::Invalid | LogicalType::SqlNull => Err(TypeError::InternalError(format!(
                "Unsupported type {:?} for builder",
                data_type
            ))),
            LogicalType::Boolean => Ok(Box::new(BooleanBuilder::new())),
            LogicalType::Tinyint => Ok(Box::new(Int8Builder::new())),
            LogicalType::UTinyint => Ok(Box::new(UInt8Builder::new())),
            LogicalType::Smallint => Ok(Box::new(Int16Builder::new())),
            LogicalType::USmallint => Ok(Box::new(UInt16Builder::new())),
            LogicalType::Integer => Ok(Box::new(Int32Builder::new())),
            LogicalType::UInteger => Ok(Box::new(UInt32Builder::new())),
            LogicalType::Bigint => Ok(Box::new(Int64Builder::new())),
            LogicalType::UBigint => Ok(Box::new(UInt64Builder::new())),
            LogicalType::Float => Ok(Box::new(Float32Builder::new())),
            LogicalType::Double => Ok(Box::new(Float64Builder::new())),
            LogicalType::Varchar => Ok(Box::new(StringBuilder::new())),
            LogicalType::Date => Ok(Box::new(Date32Builder::new())),
            LogicalType::Interval(IntervalUnit::DayTime) => {
                Ok(Box::new(IntervalDayTimeBuilder::new()))
            }
            LogicalType::Interval(IntervalUnit::YearMonth) => {
                Ok(Box::new(IntervalYearMonthBuilder::new()))
            }
            LogicalType::Interval(IntervalUnit::MonthDayNano) => {
                Ok(Box::new(IntervalMonthDayNanoBuilder::new()))
            }
        }
    }

    pub fn append_for_builder(
        value: &ScalarValue,
        builder: &mut Box<dyn ArrayBuilder>,
    ) -> Result<(), TypeError> {
        match value {
            ScalarValue::Null => {
                return Err(TypeError::InternalError(
                    "Unsupported type: Null for builder".to_string(),
                ))
            }
            ScalarValue::Boolean(v) => builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap()
                .append_option(*v),
            ScalarValue::Utf8(v) => builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap()
                .append_option(v.as_ref()),
            ScalarValue::Int8(v) => builder
                .as_any_mut()
                .downcast_mut::<Int8Builder>()
                .unwrap()
                .append_option(*v),
            ScalarValue::Int16(v) => builder
                .as_any_mut()
                .downcast_mut::<Int16Builder>()
                .unwrap()
                .append_option(*v),
            ScalarValue::Int32(v) => builder
                .as_any_mut()
                .downcast_mut::<Int32Builder>()
                .unwrap()
                .append_option(*v),
            ScalarValue::Int64(v) => builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .unwrap()
                .append_option(*v),
            ScalarValue::UInt8(v) => builder
                .as_any_mut()
                .downcast_mut::<UInt8Builder>()
                .unwrap()
                .append_option(*v),
            ScalarValue::UInt16(v) => builder
                .as_any_mut()
                .downcast_mut::<UInt16Builder>()
                .unwrap()
                .append_option(*v),
            ScalarValue::UInt32(v) => builder
                .as_any_mut()
                .downcast_mut::<UInt32Builder>()
                .unwrap()
                .append_option(*v),
            ScalarValue::UInt64(v) => builder
                .as_any_mut()
                .downcast_mut::<UInt64Builder>()
                .unwrap()
                .append_option(*v),
            ScalarValue::Float32(v) => builder
                .as_any_mut()
                .downcast_mut::<Float32Builder>()
                .unwrap()
                .append_option(*v),
            ScalarValue::Float64(v) => builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap()
                .append_option(*v),
            ScalarValue::Date32(v) => builder
                .as_any_mut()
                .downcast_mut::<Date32Builder>()
                .unwrap()
                .append_option(*v),
            ScalarValue::IntervalYearMonth(v) => builder
                .as_any_mut()
                .downcast_mut::<IntervalYearMonthBuilder>()
                .unwrap()
                .append_option(*v),
            ScalarValue::IntervalDayTime(v) => builder
                .as_any_mut()
                .downcast_mut::<IntervalDayTimeBuilder>()
                .unwrap()
                .append_option(*v),
        }
        Ok(())
    }

    pub fn get_datatype(&self) -> DataType {
        match self {
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::Null => DataType::Null,
            ScalarValue::Date32(_) => DataType::Date32,
            ScalarValue::IntervalYearMonth(_) => DataType::Interval(IntervalUnit::YearMonth),
            ScalarValue::IntervalDayTime(_) => DataType::Interval(IntervalUnit::DayTime),
        }
    }

    /// This method is to eliminate unnecessary type conversion
    /// TODO: enhance this to support more types
    pub fn cast_to_type(&self, cast_type: &DataType) -> Option<ScalarValue> {
        match (self, cast_type) {
            (ScalarValue::Int32(v), DataType::Int64) => {
                v.map(|v| ScalarValue::Int64(Some(v as i64)))
            }
            (ScalarValue::Int32(v), DataType::Float64) => {
                v.map(|v| ScalarValue::Float64(Some(v as f64)))
            }
            _ => None,
        }
    }

    pub fn as_usize(&self) -> Option<usize> {
        match self {
            ScalarValue::Int64(Some(v)) => Some(*v as usize),
            ScalarValue::Int32(Some(v)) => Some(*v as usize),
            _ => None,
        }
    }
}

macro_rules! impl_scalar {
    ($ty:ty, $scalar:tt) => {
        impl From<$ty> for ScalarValue {
            fn from(value: $ty) -> Self {
                ScalarValue::$scalar(Some(value))
            }
        }

        impl From<Option<$ty>> for ScalarValue {
            fn from(value: Option<$ty>) -> Self {
                ScalarValue::$scalar(value)
            }
        }
    };
}

impl_scalar!(f64, Float64);
impl_scalar!(f32, Float32);
impl_scalar!(i8, Int8);
impl_scalar!(i16, Int16);
impl_scalar!(i32, Int32);
impl_scalar!(i64, Int64);
impl_scalar!(bool, Boolean);
impl_scalar!(u8, UInt8);
impl_scalar!(u16, UInt16);
impl_scalar!(u32, UInt32);
impl_scalar!(u64, UInt64);
impl_scalar!(String, Utf8);

impl From<&sqlparser::ast::Value> for ScalarValue {
    fn from(v: &sqlparser::ast::Value) -> Self {
        match v {
            sqlparser::ast::Value::Number(n, _) => {
                // use i32 to handle most cases
                if let Ok(v) = n.parse::<i32>() {
                    v.into()
                } else if let Ok(v) = n.parse::<i64>() {
                    v.into()
                } else if let Ok(v) = n.parse::<f32>() {
                    v.into()
                } else if let Ok(v) = n.parse::<f64>() {
                    v.into()
                } else {
                    panic!("unsupported number {:?}", n)
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s) => s.clone().into(),
            sqlparser::ast::Value::DoubleQuotedString(s) => s.clone().into(),
            sqlparser::ast::Value::Boolean(b) => (*b).into(),
            sqlparser::ast::Value::Null => Self::Null,
            _ => todo!("unsupported parsed scalar value {:?}", v),
        }
    }
}

macro_rules! format_option {
    ($F:expr, $EXPR:expr) => {{
        match $EXPR {
            Some(e) => write!($F, "{}", e),
            None => write!($F, "NULL"),
        }
    }};
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ScalarValue::Boolean(e) => format_option!(f, e)?,
            ScalarValue::Float32(e) => format_option!(f, e)?,
            ScalarValue::Float64(e) => format_option!(f, e)?,
            ScalarValue::Int8(e) => format_option!(f, e)?,
            ScalarValue::Int16(e) => format_option!(f, e)?,
            ScalarValue::Int32(e) => format_option!(f, e)?,
            ScalarValue::Int64(e) => format_option!(f, e)?,
            ScalarValue::UInt8(e) => format_option!(f, e)?,
            ScalarValue::UInt16(e) => format_option!(f, e)?,
            ScalarValue::UInt32(e) => format_option!(f, e)?,
            ScalarValue::UInt64(e) => format_option!(f, e)?,
            ScalarValue::Utf8(e) => format_option!(f, e)?,
            ScalarValue::Null => write!(f, "NULL")?,
            ScalarValue::Date32(e) => format_option!(f, e)?,
            ScalarValue::IntervalDayTime(e) => format_option!(f, e)?,
            ScalarValue::IntervalYearMonth(e) => format_option!(f, e)?,
        };
        Ok(())
    }
}

impl fmt::Debug for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ScalarValue::Boolean(_) => write!(f, "Boolean({})", self),
            ScalarValue::Float32(_) => write!(f, "Float32({})", self),
            ScalarValue::Float64(_) => write!(f, "Float64({})", self),
            ScalarValue::Int8(_) => write!(f, "Int8({})", self),
            ScalarValue::Int16(_) => write!(f, "Int16({})", self),
            ScalarValue::Int32(_) => write!(f, "Int32({})", self),
            ScalarValue::Int64(_) => write!(f, "Int64({})", self),
            ScalarValue::UInt8(_) => write!(f, "UInt8({})", self),
            ScalarValue::UInt16(_) => write!(f, "UInt16({})", self),
            ScalarValue::UInt32(_) => write!(f, "UInt32({})", self),
            ScalarValue::UInt64(_) => write!(f, "UInt64({})", self),
            ScalarValue::Utf8(None) => write!(f, "Utf8({})", self),
            ScalarValue::Utf8(Some(_)) => write!(f, "Utf8(\"{}\")", self),
            ScalarValue::Null => write!(f, "NULL"),
            ScalarValue::Date32(_) => write!(f, "Date32({})", self),
            ScalarValue::IntervalYearMonth(_) => write!(f, "IntervalYearMonth({})", self),
            ScalarValue::IntervalDayTime(_) => write!(f, "IntervalDayTime({})", self),
        }
    }
}

pub fn build_scalar_value_builder(data_type: &DataType) -> Box<dyn ArrayBuilder> {
    match data_type {
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        _ => panic!("Unsupported data type: {}", data_type),
    }
}
